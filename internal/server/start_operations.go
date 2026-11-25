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
	"strings"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/pkg/sandbox"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const startKeyLabel = "sandboxd.start_key"

type startOperation struct {
	done      chan struct{}
	cancel    context.CancelFunc
	sandboxID string
	fenced    bool
	pending   *runtime.SandboxStatus
 cleanup *startCleanup
 cleanupGate chan struct{}
}

func validStartKey(key string) bool {
	return key != "" && len(key) <= 255 && strings.TrimSpace(key) == key
}

// beginStart publishes the operation before any preparation can run. The finish
// callback runs after Start's rollback defers, not when the client disconnects.
func (h *sandboxService) beginStart(ctx context.Context, key string) (context.Context, func(), error) {
	if key == "" {
		return ctx, func() {}, nil
	}
	if !validStartKey(key) {
		return nil, nil, status.Error(codes.InvalidArgument, "invalid start key")
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	if h.startOperations == nil {
		h.startOperations = make(map[string]*startOperation)
	}
	if op := h.startOperations[key]; op != nil {
		return nil, nil, status.Error(codes.AlreadyExists, "start key is already used or fenced")
	}
	child, cancel := context.WithCancel(ctx)
	op := &startOperation{done: make(chan struct{}), cancel: cancel, cleanupGate: make(chan struct{}, 1)}
	h.startOperations[key] = op
	return child, func() {
		cancel()
		h.startMu.Lock()
		op.cancel = nil
		if op.cleanup == nil { op.pending = nil }
		close(op.done)
		h.startMu.Unlock()
	}, nil
}

func (h *sandboxService) setStartSandbox(key, id string) {
	if key == "" {
		return
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	h.startOperations[key].sandboxID = id
}

// deleteStart installs a fence even when Delete arrives before Start. A client
// timeout leaves that fence in place; a retry waits on the same completion.
// Fences live until sandboxd exits. Keys must identify unique execution attempts.
func (h *sandboxService) deleteStart(ctx context.Context, req *runtime.DeleteRequest) (*runtime.DeleteResponse, error) {
	if !validStartKey(req.StartKey) || req.ID != "" {
		return nil, status.Error(codes.InvalidArgument, "start-key deletion requires a key and no sandbox ID")
	}
	if !h.recoveryReady.Load() {
		return nil, status.Error(codes.Unavailable, "sandbox recovery is incomplete")
	}
	h.startMu.Lock()
	if h.startOperations == nil {
		h.startOperations = make(map[string]*startOperation)
	}
	op := h.startOperations[req.StartKey]
	if op == nil {
		op = &startOperation{done: make(chan struct{}), cleanupGate: make(chan struct{}, 1)}
		close(op.done)
		h.startOperations[req.StartKey] = op
	}
	op.fenced = true
	cancel := op.cancel
	h.startMu.Unlock()
	if cancel != nil {
		cancel()
	}
	select {
	case <-op.done:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
 // Serialize cleanup only within this exact operation. Caller timeout cannot
 // permit a second cleaner to race a handler that ignores cancellation.
 select {
 case op.cleanupGate <- struct{}{}:
  defer func() { <-op.cleanupGate }()
 case <-ctx.Done(): return nil, status.FromContextError(ctx.Err()).Err()
 }
 if err := ctx.Err(); err != nil { return nil, status.FromContextError(err).Err() }
 // The ID reservation proves an unpublished native's ownership. Metadata must
 // also match whenever present, so an old key never deletes a reused ID.
 h.startMu.Lock()
 cleanup, id := op.cleanup, op.sandboxID
 h.startMu.Unlock()
 if cleanup != nil {
  if current, err := h.sandboxManager.Get(id); err == nil && (current.Metadata == nil || current.Metadata.Labels[startKeyLabel] != req.StartKey) {
   return nil, status.Error(codes.FailedPrecondition, "sandbox belongs to a different start attempt")
  }
  if err := h.cleanupStart(ctx, cleanup); err != nil { return nil, err }
  h.setStartCleanup(req.StartKey, nil)
  id = ""
 }
 ids := make(map[string]struct{})
 if id != "" { ids[id] = struct{}{} }
 for _, s := range h.sandboxManager.List(sandbox.ListFilterByLabels(map[string]string{startKeyLabel: req.StartKey})) {
  if s != nil && s.Metadata != nil { ids[s.Metadata.ID] = struct{}{} }
 }
 for id := range ids {
  current, err := h.sandboxManager.Get(id)
  if err != nil { continue }
  if current.Metadata == nil || current.Metadata.Labels[startKeyLabel] != req.StartKey {
   return nil, status.Error(codes.FailedPrecondition, "sandbox belongs to a different start attempt")
  }
  cleanup, err := h.recoveredStartCleanup(id, current.Metadata)
  if err != nil { return nil, err }
  h.setStartCleanup(req.StartKey, cleanup)
  if err := h.cleanupStart(ctx, cleanup); err != nil { return nil, err }
  h.setStartCleanup(req.StartKey, nil)
 }
 if err := ctx.Err(); err != nil { return nil, status.FromContextError(err).Err() }
	// The acknowledgement prevents an older server that ignores start_key from
	// being mistaken for a server that fenced an unknown attempt.
	if err := grpc.SetHeader(ctx, metadata.Pairs("sandboxd-start-fenced", "true")); err != nil {
		return nil, status.Error(codes.Internal, "cannot acknowledge start fence")
	}
	return &runtime.DeleteResponse{}, nil
}

// Keep terminal keys for process lifetime. Evicting them would let a delayed
// Start recreate an execution after successful retirement. Heavy cleanup state
// and inventory snapshots are dropped once physical cleanup is confirmed.
func (h *sandboxService) setStartCleanup(key string, cleanup *startCleanup) {
 h.startMu.Lock()
 defer h.startMu.Unlock()
 op := h.startOperations[key]
 op.cleanup = cleanup
 if cleanup == nil {
  op.sandboxID = ""
  op.pending = nil
 }
}

// The inventory snapshot and finishStart share startMu: an attempt cannot
// disappear between reading committed metadata and copying its pending status.
func (h *sandboxService) pendingInventoryLocked(req *runtime.ListSandboxesRequest) []*runtime.SandboxStatus {
	var result []*runtime.SandboxStatus
	for _, op := range h.startOperations {
		if (op.cancel == nil && op.cleanup == nil) || op.pending == nil || (req.ID != "" && req.ID != op.sandboxID) {
			continue
		}
		matches := true
		if req.ID == "" {
			for k, v := range req.Selector {
				if op.pending.Labels[k] != v {
					matches = false
					break
				}
			}
		}
		if matches {
			result = append(result, &runtime.SandboxStatus{ID: op.pending.ID, Runtime: op.pending.Runtime, State: runtime.SandboxState_SANDBOX_STATE_UNKNOWN, Labels: copyStringMap(op.pending.Labels), MetricLabels: copyStringMap(op.pending.MetricLabels), Stdout: op.pending.Stdout, Stderr: op.pending.Stderr})
		}
	}
	return result
}

func (h *sandboxService) setStartInventory(req *runtime.StartRequest) {
	if req.StartKey == "" {
		return
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	h.startOperations[req.StartKey].pending = &runtime.SandboxStatus{ID: req.SandboxID, Runtime: req.Runtime, Labels: copyStringMap(req.Labels), MetricLabels: copyStringMap(req.MetricLabels), Stdout: req.Stdout, Stderr: req.Stderr}
}

func (h *sandboxService) activeStartKey(id string) string {
	h.startMu.Lock()
	defer h.startMu.Unlock()
	for key, op := range h.startOperations {
		if (op.cancel != nil || op.cleanup != nil) && op.sandboxID == id {
			return key
		}
	}
	return ""
}
