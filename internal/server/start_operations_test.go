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
	"net"
	"sync"
	"strings"
	"testing"
	"time"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/config"
	"github.com/inclusionAI/sandboxd/internal/testregistry"
	imageapi "github.com/inclusionAI/sandboxd/pkg/imagemanager/api"
	"github.com/inclusionAI/sandboxd/pkg/imagemanager/oci"
	"github.com/inclusionAI/sandboxd/pkg/networkmanager"
	"github.com/inclusionAI/sandboxd/pkg/sandbox"
	svc "github.com/inclusionAI/sandboxd/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func fenceContext() context.Context {
	return grpc.NewContextWithServerTransportStream(context.Background(), &startFailureStream{})
}

func TestDeleteStartKeyFencesRequestThatHasNotArrived(t *testing.T) {
	s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunc: svc.NewFakeRuntimeHandler()})
	for i := 0; i < 2; i++ {
		if _, err := s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: "late-attempt"}); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.Start(context.Background(), &runtime.StartRequest{StartKey: "late-attempt"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("late Start was not fenced: %v", err)
	}
}

func TestDeleteStartKeyCancelsBlockedOCIPreparation(t *testing.T) {
	checkBlockedStartCleanup(t, false)
}

func TestRestartInventoryFindsAndCancelsPendingStartByPhysicalID(t *testing.T) {
	checkBlockedStartCleanup(t, true)
}

func checkBlockedStartCleanup(t *testing.T, physical bool) {
	t.Helper()
	registry := testregistry.New(t, "manifest")
	manager, err := oci.NewManager(t.TempDir(), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.Close() })
	worker, err := imageapi.NewHttpWorker(&imageapi.HttpWorkerConfig{OCIManager: manager})
	if err != nil {
		t.Fatal(err)
	}
	handler := &cancelStartHandler{Handler: svc.NewFakeRuntimeHandler()}
	s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunc: handler})
	s.fsMgr = newFSManager(worker)
	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()
	done := make(chan error, 1)
	go func() {
		_, err := s.Start(startCtx, &runtime.StartRequest{StartKey: "blocked-attempt", Runtime: config.RuntimeNameRunc, Rootfs: &runtime.RootfsConfig{Type: runtime.RootfsSrcType_IMAGE, Source: &runtime.RootfsConfig_ImageUrl{ImageUrl: registry.ImageURL}}})
		done <- err
	}()
	select {
	case <-registry.Entered:
	case <-time.After(5 * time.Second):
		t.Fatal("download not reached")
	}
	deletion := &runtime.DeleteRequest{StartKey: "blocked-attempt"}
	if physical {
		// A restarted caller has lost the Start key. Its authoritative inventory
		// must still see the in-flight attempt before it commits a real sandbox.
		inventory, err := s.List(context.Background(), &runtime.ListSandboxesRequest{})
		if err != nil || len(inventory.Sandboxes) != 1 {
			t.Fatalf("pending Start is absent from recovery inventory: %v %v", inventory, err)
		}
		pending := inventory.Sandboxes[0]
		if pending.State != runtime.SandboxState_SANDBOX_STATE_UNKNOWN || pending.Labels[startKeyLabel] != "blocked-attempt" {
			t.Fatal(pending)
		}
		deletion = &runtime.DeleteRequest{ID: pending.ID}
	}
	if _, err := s.Delete(fenceContext(), deletion); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start is still executing")
	}
	if handler.starts.Load() != 0 || len(s.fsMgr.rootfs.List()) != 0 || len(s.fsMgr.sandboxState) != 0 {
		t.Fatal("start or preparation ownership remains")
	}
	_, err = s.Start(context.Background(), &runtime.StartRequest{StartKey: "blocked-attempt"})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("cancelled attempt was repeated: %v", err)
	}
}

func TestDeleteStartKeyWaitsForLateCompletionAndCanRetry(t *testing.T) {
	handler := &recordingDeleteHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler()}
	s := newTestService(t, map[string]svc.Handler{"runsc": handler})
	child, finish, err := s.beginStart(context.Background(), "slow-attempt")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(fenceContext(), 20*time.Millisecond)
	defer cancel()
	if _, err = s.Delete(ctx, &runtime.DeleteRequest{StartKey: "slow-attempt"}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Delete overtook Start: %v", err)
	}
	if child.Err() != context.Canceled {
		t.Fatal("Start was not cancelled")
	}
	// Model a backend that completes after cancellation. Publication occurs before
	// the handler signals completion, so a retry must remove this late execution.
	const id = "sbox-late-completion"
	storeSandboxForDelete(t, s, id)
	current, err := s.sandboxManager.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	current.Metadata.Labels = map[string]string{startKeyLabel: "slow-attempt"}
	s.setStartSandbox("slow-attempt", id)
	finish()
	if _, err = s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: "slow-attempt"}); err != nil {
		t.Fatal(err)
	}
	if handler.calls != 1 || handler.sandboxID != id {
		t.Fatal("late runtime was not deleted")
	}
	if _, err = s.sandboxManager.Get(id); err == nil {
		t.Fatal("late metadata remains")
	}
}

func TestDeleteStartKeyRecoversGeneratedIdentityAfterRestart(t *testing.T) {
	handler := &recordingDeleteHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler()}
	s := newTestService(t, map[string]svc.Handler{"runsc": handler})
	const id = "sbox-recovered-start"
	storeSandboxForDelete(t, s, id)
	sandbox, err := s.sandboxManager.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	sandbox.Metadata.Labels = map[string]string{startKeyLabel: "recovered-attempt"}
	if err = s.sandboxManager.StoreMetadata(id, sandbox.Metadata); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: "recovered-attempt"}); err != nil {
		t.Fatal(err)
	}
	if handler.calls != 1 || handler.sandboxID != id {
		t.Fatal("recovered runtime remains")
	}
}

func TestDeleteStartKeyPreservesReusedPhysicalIdentity(t *testing.T) {
	handler := &recordingDeleteHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler()}
	s := newTestService(t, map[string]svc.Handler{"runsc": handler})
	_, finish, err := s.beginStart(context.Background(), "failed-attempt")
	if err != nil {
		t.Fatal(err)
	}
	const id = "sbox-reused"
	s.setStartSandbox("failed-attempt", id)
	finish()
	storeSandboxForDelete(t, s, id)
	_, err = s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: "failed-attempt"})
	if status.Code(err) != codes.FailedPrecondition || handler.calls != 0 {
		t.Fatalf("reused identity was not preserved: %v", err)
	}
}

// A native runtime exists before metadata publication. Its failed rollback must
// remain owned by the exact attempt, even though ordinary inventory is empty.
type failedRollbackHandler struct {
 *svc.FakeRuntimeHandler
 mu sync.Mutex
 native string
 failDelete bool
 deletes int
}

func (h *failedRollbackHandler) Start(_ context.Context, cfg svc.StartConfig) error {
 h.mu.Lock()
 defer h.mu.Unlock()
 h.native = cfg.ID
 return nil
}

func (h *failedRollbackHandler) Delete(_ context.Context, id string) error {
 h.mu.Lock()
 defer h.mu.Unlock()
 h.deletes++
 if h.failDelete { return errors.New("injected native delete failure") }
 if h.native != id { return errors.New("wrong native identity") }
 h.native = ""
 return nil
}

type fenceHeaderStream struct {
 startFailureStream
 headers metadata.MD
}

func (s *fenceHeaderStream) SetHeader(md metadata.MD) error {
 s.headers = metadata.Join(s.headers, md)
 return nil
}

func TestDeleteStartKeyRetainsFailedNativeRollbackUntilRetry(t *testing.T) {
 handler := &failedRollbackHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler(), failDelete: true}
 s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: handler})
 useFakeStartResources(t, s)
 const id = "sbox-failed-native-rollback"
 const key = "failed-native-rollback"
 _, err := s.Start(context.Background(), &runtime.StartRequest{
  SandboxID: id, StartKey: key, Runtime: config.RuntimeNameRunsc,
  Rootfs: &runtime.RootfsConfig{Type: runtime.RootfsSrcType_LOCAL, Source: &runtime.RootfsConfig_Path{Path: t.TempDir()}},
  Ports: []string{"8080:80"},
 })
 if err == nil || !strings.Contains(err.Error(), "sandbox IP not available") { t.Fatalf("post-start failure not reached: %v", err) }
 if handler.native != id || handler.deletes != 1 { t.Fatalf("native rollback was not attempted: %+v", handler) }
 if _, err := s.sandboxManager.Get(id); err == nil { t.Fatal("test must fail before metadata publication") }
 stream := &fenceHeaderStream{}
 ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
 if _, err = s.Delete(ctx, &runtime.DeleteRequest{StartKey: key}); err == nil { t.Error("key Delete acknowledged failed native cleanup") }
 if len(stream.headers.Get("sandboxd-start-fenced")) != 0 { t.Error("failed cleanup sent fence acknowledgement") }
 if _, err := s.sandboxManager.ReserveID(id); err == nil { t.Error("failed native cleanup released its physical identity"); s.sandboxManager.ReleaseID(id) }
 if len(s.fsMgr.rootfs.List()) != 1 { t.Error("failed native cleanup released rootfs ownership") }
 handler.failDelete = false
 stream = &fenceHeaderStream{}
 ctx = grpc.NewContextWithServerTransportStream(context.Background(), stream)
 if _, err = s.Delete(ctx, &runtime.DeleteRequest{StartKey: key}); err != nil { t.Fatal(err) }
 if got := stream.headers.Get("sandboxd-start-fenced"); len(got) != 1 || got[0] != "true" { t.Fatal("successful retry did not acknowledge fence") }
 if handler.native != "" || handler.deletes != 3 || len(s.fsMgr.rootfs.List()) != 0 { t.Fatalf("cleanup remains: native=%s deletes=%d rootfs=%d", handler.native, handler.deletes, len(s.fsMgr.rootfs.List())) }
 if _, err := s.sandboxManager.ReserveID(id); err != nil { t.Fatalf("successful cleanup did not release ID: %v", err) }
 // A later execution can reuse the physical identity. Retrying the old key
 // must never route deletion to the new runtime.
 storeSandboxForDelete(t, s, id)
 _, _ = s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: key})
 if handler.deletes != 3 { t.Fatal("old fence deleted a reused physical identity") }
}

func useFakeStartResources(t *testing.T, s *sandboxService) {
 t.Helper()
 // Avoid host network/cgroup mutation while exercising real Start/rollback.
 patch := gomonkey.ApplyPrivateMethod(s, "prepareStartResourcesForAttempt", func(_ *sandboxService, _, id string, _ bool) (*preparedStartResources, error) {
  return &preparedStartResources{OccupiedResource: sandbox.OccupiedResource{ID: id, Resources: map[string]string{}}, network: &networkmanager.NetResource{Ip: net.ParseIP("10.0.0.2")}}, nil
 })
 t.Cleanup(patch.Reset)
}

type failedRootfsRelease struct {
 *fsTestImageService
 fail bool
 releases int
}

func (s *failedRootfsRelease) UmountOCI(req *imageapi.OCIUmountRequest) error {
 s.releases++
 if s.fail { return errors.New("injected rootfs release failure") }
 return s.fsTestImageService.UmountOCI(req)
}

func TestDeleteStartKeyRetainsFailedRootfsReleaseUntilRetry(t *testing.T) {
 handler := &failedRollbackHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler()}
 s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: handler})
 useFakeStartResources(t, s)
 images := &failedRootfsRelease{fsTestImageService: newFSTestImageService(), fail: true}
 s.fsMgr = newFSManager(images)
 req := fsTestStartRequest("sbox-failed-rootfs-release")
 req.StartKey, req.Runtime, req.Mounts, req.Ports = "failed-rootfs-release", config.RuntimeNameRunsc, nil, []string{"8080:80"}
 if _, err := s.Start(context.Background(), req); err == nil || !strings.Contains(err.Error(), "sandbox IP not available") { t.Fatalf("post-start failure not reached: %v", err) }
 if handler.native != "" || handler.deletes != 1 { t.Fatal("native must be deleted before filesystem release") }
 stream := &fenceHeaderStream{}
 ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
 if _, err := s.Delete(ctx, &runtime.DeleteRequest{StartKey: req.StartKey}); err == nil { t.Fatal("failed rootfs release was acknowledged") }
 if len(stream.headers.Get("sandboxd-start-fenced")) != 0 { t.Fatal("failed rootfs release sent ACK") }
 if _, err := s.sandboxManager.ReserveID(req.SandboxID); err == nil { t.Fatal("rootfs release failure released ID") }
 images.fail = false
 if _, err := s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: req.StartKey}); err != nil { t.Fatal(err) }
 if handler.deletes != 1 || images.releases != 3 || len(s.fsMgr.rootfs.List()) != 0 { t.Fatalf("retry did not consume exact outstanding resource: deletes=%d releases=%d rootfs=%d", handler.deletes, images.releases, len(s.fsMgr.rootfs.List())) }
}

func TestStartKeyRejectsUnsupportedPreparationBeforeAcquisition(t *testing.T) {
 s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
 for _, req := range []*runtime.StartRequest{
  {StartKey: "key-mount", Mounts: []*runtime.Mount{{Target: "/volume"}}},
  {StartKey: "key-restore", CheckpointInfo: &runtime.CheckpointInfo{}},
  {StartKey: "key-s3", Rootfs: &runtime.RootfsConfig{Type: runtime.RootfsSrcType_S3}},
 } {
  if _, err := s.Start(context.Background(), req); status.Code(err) != codes.Unimplemented { t.Fatalf("unsupported start acquired resources: %v", err) }
 }
 if len(s.startOperations) != 0 || len(s.fsMgr.rootfs.List()) != 0 { t.Fatal("unsupported preparation acquired ownership") }
}

func TestStartKeyAllowsReadOnlyLocalBootstrapMounts(t *testing.T) {
 for _, typ := range []string{"erofs", "bind"} {
  req := &runtime.StartRequest{StartKey: "bootstrap-"+typ, Mounts: []*runtime.Mount{{
   Type: typ, Target: "/__adx", Options: []string{"ro", "rbind"}, Source: &runtime.Mount_HostPath{HostPath: "/opt/bootstrap"},
  }}}
  if err := validateKeyedStart(req); err != nil { t.Fatalf("deployment bootstrap was rejected: %v", err) }
 }
}

func TestStartKeyDnatFailureRetainsAttemptedRulesForCleanupRetry(t *testing.T) {
 fake := &fakeNetworkManager{failNext: true}
 s := newDnatTestService(t, fake)
 const id = "sbox-keyed-dnat"
 if err := s.networkMgr.setupDnatRulesOwned(id, []string{"tcp:8080:80"}, "10.0.0.2"); err == nil { t.Fatal("injected setup failure did not occur") }
 if len(s.networkMgr.rulesFor(id)) != 1 { t.Fatal("attempted rule was forgotten after setup failure") }
 fake.failNext = true
 if err := s.networkMgr.cleanupDnatRulesChecked(id); err == nil { t.Fatal("injected cleanup failure did not occur") }
 if len(s.networkMgr.rulesFor(id)) != 1 { t.Fatal("failed cleanup lost the attempted rule") }
 if err := s.networkMgr.cleanupDnatRulesChecked(id); err != nil { t.Fatal(err) }
 if len(s.networkMgr.rulesFor(id)) != 0 { t.Fatal("successful cleanup did not consume the attempted rule") }
}

func TestDeleteStartKeyAcknowledgesProcessIncarnation(t *testing.T) {
 first := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
 replacement := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
 acknowledge := func(s *sandboxService, key string) string {
  t.Helper()
  stream := &fenceHeaderStream{}
  ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
  if _, err := s.Delete(ctx, &runtime.DeleteRequest{StartKey: key}); err != nil { t.Fatal(err) }
  values := stream.headers.Get("sandboxd-start-fence-instance")
  if len(values) != 1 || len(values[0]) != 32 { t.Fatalf("missing process incarnation: %v", values) }
  return values[0]
 }
 initial := acknowledge(first, "incarnation-probe")
 if retry := acknowledge(first, "incarnation-probe"); retry != initial { t.Fatal("same-process retry changed incarnation") }
 if other := acknowledge(first, "another-key"); other != initial { t.Fatal("same registry changed incarnation for another key") }
 if restarted := acknowledge(replacement, "incarnation-probe"); restarted == initial { t.Fatal("replacement daemon reused an old cleanup proof") }
}

type failedFenceEntropy struct{}
func (failedFenceEntropy) Read([]byte) (int, error) { return 0, errors.New("injected entropy failure") }

func TestStartKeyFenceEntropyFailureReturnsStructuredError(t *testing.T) {
 instance, err := newStartFenceInstance(failedFenceEntropy{})
 if instance != "" || status.Code(err) != codes.Internal { t.Fatalf("entropy failure must return Internal without a proof: %q %v", instance, err) }
}
