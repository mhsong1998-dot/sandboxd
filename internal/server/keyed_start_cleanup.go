// Copyright (c) 2026 Ant Group Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
 "context"
 "errors"
 "fmt"
 "os"
 "slices"

 runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
 "github.com/inclusionAI/sandboxd/pkg/errord"
 "github.com/inclusionAI/sandboxd/config"
 "github.com/inclusionAI/sandboxd/pkg/networkmanager"
 "google.golang.org/grpc/codes"
 "google.golang.org/grpc/status"
)

// startCleanup owns the exact native attempt and its unreleased resources, even
// when Start failed before metadata publication. Only its operation's cleanup
// gate may access it, after op.done closes (or from Start's final rollback).
type startCleanup struct {
 id, runtimeName string
 native bool
 fs *preparedFS
 fsCommitted bool
 resources *preparedStartResources
 files *preparedSandboxFiles
 dnat, acl, xpu bool
 quarantineEndpoint bool
 managedOwner string
 managedRollback func()
}

// Keyed retirement currently supports ordinary starts. Existing special-volume,
// checkpoint and managed-runtime requests retain the unkeyed API contract.
func validateKeyedStart(req *runtime.StartRequest) error {
 if req.StartKey == "" { return nil }
 profile, err := managedProfileSelection(req)
 if err != nil { return err }
 if req.CheckpointInfo != nil || profile != "" || req.Rootfs.GetType() == runtime.RootfsSrcType_S3 {
  return status.Error(codes.Unimplemented, "start-key retirement does not support S3, checkpoints or managed runtime profiles")
 }
 for _, mount := range req.Mounts {
  if mount.GetHostPath() == "" || !slices.Contains(mount.GetOptions(), "ro") || (mount.GetType() != "erofs" && mount.GetType() != "bind") {
   return status.Error(codes.Unimplemented, "start-key retirement requires read-only local host mounts")
  }
 }
 return nil
}

func (h *sandboxService) cleanupStart(ctx context.Context, c *startCleanup) error {
 if err := ctx.Err(); err != nil { return err }
 if c.native {
  handler, ok := h.serviceHandler.Get(c.runtimeName)
  if !ok { return status.Error(codes.Unavailable, "start runtime handler is unavailable") }
  if err := handler.Delete(ctx, c.id); err != nil && !errors.Is(err, errord.ErrNotFound) { return fmt.Errorf("delete unretired start runtime: %w", err) }
  c.native = false
  if h.resourceMod != nil { h.resourceMod.ReleaseTransientMemory(firecrackerCheckpointReservationOwner(c.id)) }
 }
 // Never release storage, endpoints, ACLs or IDs while a native runtime can
 // still refer to them. Successful steps are consumed exactly once on retries.
 if c.dnat {
  if err := h.networkMgr.cleanupDnatRulesChecked(c.id); err != nil { return err }
  c.dnat = false
 }
 if c.resources != nil {
  if err := h.deactivateStartNetwork(c.resources.OccupiedResource); err != nil { return err }
 }
 if c.acl && h.aclMgr != nil {
  h.aclMu.Lock()
  err := h.aclMgr.Remove(c.id)
  h.aclMu.Unlock()
  if err != nil { return fmt.Errorf("remove start ACL: %w", err) }
  c.acl = false
 }
 if c.quarantineEndpoint && c.resources != nil {
  endpoint := c.resources.Resources[config.ResourceNameInterface]
  if err := h.networkMgr.Discard(endpoint); err != nil { return fmt.Errorf("discard uncertain ACL endpoint: %w", err) }
  delete(c.resources.Resources, config.ResourceNameInterface)
  c.quarantineEndpoint = false
 }
 if c.fs != nil {
  if err := c.fs.releaseChecked(); err != nil { return err }
  if c.fsCommitted {
   if err := h.fsMgr.forgetReleasedStart(c.id); err != nil { return err }
  }
  c.fs = nil
 }
 if c.resources != nil {
  for name, value := range c.resources.Resources {
   if err := h.releaseStartResource(name, value); err != nil { return fmt.Errorf("release start %s: %w", name, err) }
   delete(c.resources.Resources, name)
  }
  c.resources = nil
 }
 if c.xpu && h.xpuMgr != nil { h.xpuMgr.Release(c.id); c.xpu = false }
 if err := h.releaseManagedSandbox(c.id, c.managedOwner); err != nil { return err }
 c.managedOwner = ""
 if c.managedRollback != nil { c.managedRollback(); c.managedRollback = nil }
 if c.files != nil {
  if err := os.RemoveAll(c.files.root); err != nil { return fmt.Errorf("remove start files: %w", err) }
  c.files = nil
 }
 if err := h.sandboxManager.CleanSandboxRootChecked(c.id); err != nil { return err }
 // DeleteWithContext also removes any published metadata/monitor. Its cleanup
 // is now idempotent: all physical resources and the root were confirmed gone.
 h.sandboxManager.DeleteWithContext(ctx, c.id)
 return nil
}

func (p *preparedFS) releaseChecked() error {
 if len(p.s3) != 0 { return status.Error(codes.FailedPrecondition, "S3 cleanup cannot acknowledge a start fence") }
 for len(p.oci) != 0 {
  if err := p.manager.oci.unmountOCI(p.oci[0]); err != nil { return err }
  p.oci = p.oci[1:]
 }
 if p.rootfs != nil {
  if err := p.rootfs.ReleaseChecked(); err != nil { return fmt.Errorf("release start rootfs: %w", err) }
  p.rootfs = nil
 }
 return nil
}

// The operation keeps its released preparedFS while this durable metadata
// removal retries, so a failed write cannot release the same rootfs twice.
func (m *fsManager) forgetReleasedStart(id string) error {
 result := m.writeFSState(func() (func(), error) {
  root, s3, oci, state := m.sandboxRootfs[id], m.sandboxS3Mount[id], m.sandboxOCIMount[id], m.sandboxState[id]
  _, exists := m.sandboxState[id]
  delete(m.sandboxRootfs, id)
  delete(m.sandboxS3Mount, id)
  delete(m.sandboxOCIMount, id)
  delete(m.sandboxState, id)
  return func() {
   if root != nil { m.sandboxRootfs[id] = root }
   if len(s3) != 0 { m.sandboxS3Mount[id] = s3 }
   if len(oci) != 0 { m.sandboxOCIMount[id] = oci }
   if exists { m.sandboxState[id] = state }
  }, nil
 }, true)
 return result.err
}

func (h *sandboxService) recoveredStartCleanup(id string, metadata *runtime.SandboxMetadata) (*startCleanup, error) {
 resources, err := h.sandboxManager.CollectResourceByID(id)
 if err != nil { return nil, err }
 h.fsMgr.mu.Lock()
 fs := &preparedFS{manager: h.fsMgr, rootfs: h.fsMgr.sandboxRootfs[id], s3: cloneS3Configs(h.fsMgr.sandboxS3Mount[id]), oci: append([]string(nil), h.fsMgr.sandboxOCIMount[id]...)}
 h.fsMgr.mu.Unlock()
 return &startCleanup{id: id, runtimeName: metadata.RuntimeHandler, native: true, fs: fs, fsCommitted: true,
  resources: &preparedStartResources{OccupiedResource: resources}, dnat: true, acl: h.aclMgr != nil,
  xpu: h.xpuMgr != nil, managedOwner: metadata.Labels[managedOwnerLabel]}, nil
}

func (m *networkManager) cleanupDnatRulesChecked(id string) error {
 for _, rule := range m.rulesFor(id) {
  nat, ok := networkmanager.NetworkManagers[m.natBackend]
  if !ok { return fmt.Errorf("network manager %s unavailable", m.natBackend) }
  if err := cleanupDnatRule(nat, rule); err != nil { return err }
 }
 m.dnatMu.Lock()
 delete(m.dnatRules, id)
 m.dnatMu.Unlock()
 return nil
}


// Record each attempted DNAT rule before setup. An error may follow a partial
// backend mutation; keyed rollback must retain a retryable cleanup identity.
func (m *networkManager) setupDnatRulesOwned(id string, ports []string, targetIP string) error {
 nat, ok := networkmanager.NetworkManagers[m.natBackend]
 if !ok { return fmt.Errorf("network manager %s unavailable", m.natBackend) }
 var local networkmanager.LocalDNATManager
 if m.enableLocalDNAT {
  var ok bool
  local, ok = nat.(networkmanager.LocalDNATManager)
  if !ok { return fmt.Errorf("network manager %s does not support local DNAT", m.natBackend) }
 }
 for _, port := range ports {
  rule, err := parseDnatRule(id, port, targetIP)
  if err != nil { return err }
  m.dnatMu.Lock()
  m.dnatRules[id] = append(m.dnatRules[id], rule)
  m.dnatMu.Unlock()
  if err := nat.SetupDNATRule(rule.Protocol, rule.DstPort, rule.TargetIP, rule.TargetPort); err != nil { return err }
  if local != nil {
   if err := local.SetupLocalDNATRule(rule.Protocol, rule.DstPort, rule.TargetIP, rule.TargetPort); err != nil { return err }
  }
 }
 return nil
}
