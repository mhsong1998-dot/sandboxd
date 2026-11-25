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
 "fmt"
 "sync"
 "testing"
 "time"

 gomonkey "github.com/agiledragon/gomonkey/v2"
 runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
 "github.com/inclusionAI/sandboxd/config"
 "github.com/inclusionAI/sandboxd/internal/metrics"
 svc "github.com/inclusionAI/sandboxd/pkg/runtime"
 "github.com/inclusionAI/sandboxd/pkg/sandbox"
 dto "github.com/prometheus/client_model/go"
 "google.golang.org/grpc/codes"
 "google.golang.org/grpc/status"
)

type blockedListStatus struct {
 sandbox.StatusStorage
 entered chan struct{}
 release chan struct{}
 once sync.Once
 gets int
}

func (s *blockedListStatus) Get() sandbox.Status {
 s.once.Do(func() { close(s.entered) })
 <-s.release
 s.gets++
 return s.StatusStorage.Get()
}

func TestListResponseAssemblyDoesNotBlockStartRegistry(t *testing.T) {
 s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
 const id = "sbox-list-unlocked-assembly"
 storeSandboxForDelete(t, s, id)
 current, err := s.sandboxManager.Get(id)
 if err != nil { t.Fatal(err) }
 blocked := &blockedListStatus{StatusStorage: current.Status, entered: make(chan struct{}), release: make(chan struct{})}
 current.Status = blocked
 var release sync.Once
 unblock := func() { release.Do(func() { close(blocked.release) }) }
 t.Cleanup(unblock)
 result := make(chan error, 1)
 go func() {
  response, err := s.List(context.Background(), &runtime.ListSandboxesRequest{})
  if err == nil && (len(response.Sandboxes) != 1 || response.Sandboxes[0].ID != id) { err = fmt.Errorf("list lost committed sandbox: %v", response) }
  result <- err
 }()
 <-blocked.entered
 registered := make(chan error, 1)
 go func() {
  _, finish, err := s.beginStart(context.Background(), "while-list-assembles", "")
  if err == nil { finish() }
  registered <- err
 }()
 select {
 case err := <-registered:
  if err != nil { t.Error(err) }
 case <-time.After(time.Second):
  t.Error("response assembly retained startMu while status read was blocked")
 }
 unblock()
 if err := <-result; err != nil { t.Fatal(err) }
 if blocked.gets != 1 { t.Errorf("List read %d independent status versions, want one", blocked.gets) }
}

func TestListSnapshotDoesNotMissPendingToCommittedHandoff(t *testing.T) {
 s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
 const id = "sbox-list-handoff"
 const key = "list-handoff"
 _, finish, err := s.beginStart(context.Background(), key, "")
 if err != nil { t.Fatal(err) }
 s.setStartSandbox(key, id)
 s.setStartInventory(&runtime.StartRequest{SandboxID: id, StartKey: key, Runtime: config.RuntimeNameRunsc, Labels: map[string]string{startKeyLabel: key}})
 read := make(chan struct{})
 resume := make(chan struct{})
 // The committed view was empty before publication. A correct cross-table
 // snapshot must retain the pending identity even if completion races it.
 patch := gomonkey.ApplyMethod(s.sandboxManager, "List", func(*sandbox.Manager, ...sandbox.ListOption) []*sandbox.Sandbox {
  close(read)
  <-resume
  return nil
 })
 defer patch.Reset()
 result := make(chan *runtime.ListSandboxesResponse, 1)
 go func() { response, _ := s.List(context.Background(), &runtime.ListSandboxesRequest{}); result <- response }()
 <-read
 published := make(chan struct{})
 finished := make(chan struct{})
 go func() { storeSandboxForDelete(t, s, id); close(published); finish(); close(finished) }()
 <-published
 close(resume)
 response := <-result
 <-finished
 if response == nil || len(response.Sandboxes) != 1 || response.Sandboxes[0].ID != id { t.Fatalf("cross-table handoff disappeared from inventory: %v", response) }
}

func TestListReferencesKeepMetadataImmutableAndResponsesIndependent(t *testing.T) {
 s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
 const id = "sbox-immutable-list-metadata"
 storeSandboxForDelete(t, s, id)
 old, err := s.sandboxManager.Get(id)
 if err != nil { t.Fatal(err) }
 if err := s.sandboxManager.UpdateLabels(id, map[string]string{"updated": "value"}); err != nil { t.Fatal(err) }
 if _, changed := old.Metadata.Labels["updated"]; changed { t.Error("metadata already referenced by an inventory snapshot was mutated") }
 response, err := s.List(context.Background(), &runtime.ListSandboxesRequest{})
 if err != nil || len(response.Sandboxes) != 1 { t.Fatalf("List failed: %v", err) }
 response.Sandboxes[0].Labels["updated"] = "caller-change"
 current, err := s.sandboxManager.Get(id)
 if err != nil || current.Metadata.Labels["updated"] != "value" { t.Fatal("response label map aliases committed metadata") }
}

func startResourceGauge(t *testing.T, name string) float64 {
 t.Helper()
 metric := &dto.Metric{}
 if err := metrics.ResourceGauge.WithLabelValues(name).Write(metric); err != nil { t.Fatal(err) }
 return metric.GetGauge().GetValue()
}

func TestStartTerminalKeysShareLightweightRecordAndMetrics(t *testing.T) {
 s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
 keys := []string{"compact-terminal-one", "compact-terminal-two"}
 for _, key := range keys {
  if _, err := s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: key}); err != nil { t.Fatal(err) }
 }
 first, second := s.startOperations[keys[0]], s.startOperations[keys[1]]
 if first != second || first.cleanupGate != nil || first.cleanup != nil || first.pending != nil { t.Error("retired keys still retain independent heavyweight operation records") }
 if got := startResourceGauge(t, "start_keys_active"); got != 0 { t.Errorf("active records = %v, want 0", got) }
 if got := startResourceGauge(t, "start_keys_terminal"); got != 2 { t.Errorf("terminal records = %v, want 2", got) }
 if got := startResourceGauge(t, "start_key_bytes"); got != float64(len(keys[0])+len(keys[1])) { t.Errorf("key bytes = %v", got) }
 for _, key := range keys {
  if _, _, err := s.beginStart(context.Background(), key, ""); status.Code(err) != codes.AlreadyExists { t.Fatalf("compacted terminal key accepted a late Start: %v", err) }
 }
 _, finish, err := s.beginStart(context.Background(), "another-live-key", "")
 if err != nil { t.Fatal(err) }
 if got := startResourceGauge(t, "start_keys_active"); got != 1 { t.Errorf("active records = %v, want 1", got) }
 finish()
 if got := startResourceGauge(t, "start_keys_terminal"); got != 3 { t.Errorf("finished records = %v, want 3", got) }
}
