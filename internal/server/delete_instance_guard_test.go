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
	"testing"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/config"
	svc "github.com/inclusionAI/sandboxd/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func probeStartFenceInstance(t *testing.T, s *sandboxService) string {
	t.Helper()
	stream := &fenceHeaderStream{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
	if _, err := s.Delete(ctx, &runtime.DeleteRequest{StartKey: "instance-probe"}); err != nil {
		t.Fatal(err)
	}
	values := stream.headers.Get("sandboxd-start-fence-instance")
	if len(values) != 1 || values[0] == "" {
		t.Fatalf("probe returned no instance: %v", values)
	}
	return values[0]
}

func TestDeleteRejectsStaleExpectedInstanceBeforeReusedPhysicalID(t *testing.T) {
	old := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: svc.NewFakeRuntimeHandler()})
	expected := probeStartFenceInstance(t, old)
	handler := &recordingDeleteHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler()}
	replacement := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: handler})
	current := probeStartFenceInstance(t, replacement)
	const id = "sbox-reused-after-daemon-replacement"
	storeSandboxForDelete(t, replacement, id)
	ctx := metadata.NewIncomingContext(fenceContext(), metadata.Pairs("sandboxd-expected-start-fence-instance", expected))
	if _, err := replacement.Delete(ctx, &runtime.DeleteRequest{ID: id}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("old instance deletion was not rejected before native cleanup: %v", err)
	}
	if handler.calls != 0 {
		t.Error("stale instance deletion reached the new native runtime")
	}
	if _, err := replacement.sandboxManager.Get(id); err != nil {
		t.Errorf("stale instance deletion removed reused physical identity: %v", err)
	}
	ctx = metadata.NewIncomingContext(fenceContext(), metadata.Pairs("sandboxd-expected-start-fence-instance", current))
	if _, err := replacement.Delete(ctx, &runtime.DeleteRequest{ID: id}); err != nil {
		t.Fatal(err)
	}
	if handler.calls != 1 {
		t.Errorf("current instance deletion did not clean exactly once: %d", handler.calls)
	}
}

func TestDeleteWithoutExpectedInstancePreservesLegacyBehavior(t *testing.T) {
	handler := &recordingDeleteHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler()}
	s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: handler})
	const id = "sbox-delete-without-instance"
	storeSandboxForDelete(t, s, id)
	if _, err := s.Delete(context.Background(), &runtime.DeleteRequest{ID: id}); err != nil {
		t.Fatal(err)
	}
	if handler.calls != 1 {
		t.Fatalf("legacy ID deletion changed behavior: %d", handler.calls)
	}
}
