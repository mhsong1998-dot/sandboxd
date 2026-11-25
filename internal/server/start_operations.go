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
	"crypto/rand"
	"encoding/hex"
	"io"
	"strings"
 "unsafe"

 "github.com/inclusionAI/sandboxd/internal/metrics"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/pkg/sandbox"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const startKeyLabel = "sandboxd.start_key"

type startOperation struct {
	done        chan struct{}
	cancel      context.CancelFunc
	sandboxID   string
	pending     *runtime.SandboxStatus
	cleanup     *startCleanup
	cleanupGate chan struct{}
}

// All retired keys share this immutable object. The map retains every key, but
// no per-key completion/cleanup channels survive confirmed retirement.
var terminalStartOperation = func() *startOperation {
 done := make(chan struct{})
 close(done)
 return &startOperation{done: done}
}()

func (h *sandboxService) recordStartRegistryLocked() {
 active := uint64(len(h.startOperations)) - h.startTerminalKeys
 metrics.RecordResourceGauge("start_keys_active", float64(active))
 metrics.RecordResourceGauge("start_keys_terminal", float64(h.startTerminalKeys))
 metrics.RecordResourceGauge("start_key_bytes", float64(h.startKeyBytes))
 // A lower bound, not a heap measurement: excludes map capacity, channels,
 // pending/cleanup payloads, string headers and in-flight reader references.
 metrics.RecordResourceGauge("start_operation_struct_bytes", float64((active+1)*uint64(unsafe.Sizeof(startOperation{}))))
}

func (h *sandboxService) insertStartOperationLocked(key string, op *startOperation) {
 h.startOperations[key] = op
 h.startKeyBytes += uint64(len(key))
 h.recordStartRegistryLocked()
}

func (h *sandboxService) compactStartOperationLocked(key string, op *startOperation) {
 if h.startOperations[key] != op || op == terminalStartOperation || op.cancel != nil || op.cleanup != nil || op.sandboxID != "" { return }
 if h.startKeysByID[op.sandboxID] == key { delete(h.startKeysByID, op.sandboxID) }
 op.sandboxID = ""
 op.pending = nil
 h.startOperations[key] = terminalStartOperation
 h.startTerminalKeys++
 h.recordStartRegistryLocked()
}

// A fresh registry carries a fresh process incarnation. An acknowledgement from
// a replacement daemon cannot prove cleanup of an old unpublished native.
func newStartFenceInstance(reader io.Reader) (string, error) {
	var nonce [16]byte
	if _, err := io.ReadFull(reader, nonce[:]); err != nil {
		return "", status.Error(codes.Internal, "cannot initialize start fence instance")
	}
	return hex.EncodeToString(nonce[:]), nil
}

func (h *sandboxService) initStartRegistryLocked() error {
	if h.startOperations != nil && h.startFenceInstance != "" {
		return nil
	}
	instance, err := newStartFenceInstance(rand.Reader)
	if err != nil {
		return err
	}
	h.startFenceInstance = instance
	if h.startOperations == nil {
		h.startOperations = make(map[string]*startOperation)
	}
	if h.startKeysByID == nil {
		h.startKeysByID = make(map[string]string)
	}
	return nil
}

func validStartKey(key string) bool {
	return key != "" && len(key) <= 255 && strings.TrimSpace(key) == key
}

// beginStart publishes the operation before any preparation can run. The finish
// callback runs after Start's rollback defers, not when the client disconnects.
func (h *sandboxService) beginStart(ctx context.Context, key, expectedInstance string) (context.Context, func(), error) {
	if key == "" {
		if expectedInstance != "" {
			return nil, nil, status.Error(codes.InvalidArgument, "start fence instance requires a start key")
		}
		return ctx, func() {}, nil
	}
	if !validStartKey(key) {
		return nil, nil, status.Error(codes.InvalidArgument, "invalid start key")
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	if err := h.initStartRegistryLocked(); err != nil {
		return nil, nil, err
	}
	if expectedInstance != "" && expectedInstance != h.startFenceInstance {
		return nil, nil, status.Error(codes.FailedPrecondition, "start fence instance changed")
	}
	if op := h.startOperations[key]; op != nil {
		return nil, nil, status.Error(codes.AlreadyExists, "start key is already used or fenced")
	}
	child, cancel := context.WithCancel(ctx)
	op := &startOperation{done: make(chan struct{}), cancel: cancel, cleanupGate: make(chan struct{}, 1)}
	h.insertStartOperationLocked(key, op)
	return child, func() {
		cancel()
		h.startMu.Lock()
		op.cancel = nil
		if op.cleanup == nil {
			op.pending = nil
		}
		close(op.done)
  h.compactStartOperationLocked(key, op)
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
	h.startKeysByID[id] = key
}

// A guarded one-call ID Delete must reject a replaced daemon before touching
// its physical ID. Comparing only the response nonce would be too late.
func (h *sandboxService) checkDeleteFenceInstance(ctx context.Context) error {
 values := metadata.ValueFromIncomingContext(ctx, "sandboxd-expected-start-fence-instance")
 if len(values) == 0 { return nil }
 if len(values) != 1 || values[0] == "" { return status.Error(codes.InvalidArgument, "delete fence instance requires one nonempty value") }
 h.startMu.Lock()
 defer h.startMu.Unlock()
 if err := h.initStartRegistryLocked(); err != nil { return err }
 if values[0] != h.startFenceInstance { return status.Error(codes.FailedPrecondition, "start fence instance changed before delete") }
 return nil
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
	if err := h.initStartRegistryLocked(); err != nil {
		h.startMu.Unlock()
		return nil, err
	}
	instance := h.startFenceInstance
	op := h.startOperations[req.StartKey]
 if op == terminalStartOperation {
  h.startMu.Unlock()
  return acknowledgeStartFence(ctx, instance)
 }
	if op == nil {
		op = &startOperation{done: make(chan struct{}), cleanupGate: make(chan struct{}, 1)}
		close(op.done)
		h.insertStartOperationLocked(req.StartKey, op)
	}
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
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	// The ID reservation proves an unpublished native's ownership. Metadata must
	// also match whenever present, so an old key never deletes a reused ID.
	h.startMu.Lock()
	cleanup, id := op.cleanup, op.sandboxID
	h.startMu.Unlock()
	if cleanup != nil {
		if current, err := h.sandboxManager.Get(id); err == nil && (current.Metadata == nil || current.Metadata.Labels[startKeyLabel] != req.StartKey) {
			return nil, status.Error(codes.FailedPrecondition, "sandbox belongs to a different start attempt")
		}
		if err := h.cleanupStart(ctx, cleanup); err != nil {
			return nil, err
		}
		h.setStartCleanup(req.StartKey, nil)
		id = ""
	}
	ids := make(map[string]struct{})
	if id != "" {
		ids[id] = struct{}{}
	}
	for _, s := range h.sandboxManager.List(sandbox.ListFilterByLabels(map[string]string{startKeyLabel: req.StartKey})) {
		if s != nil && s.Metadata != nil {
			ids[s.Metadata.ID] = struct{}{}
		}
	}
	for id := range ids {
		current, err := h.sandboxManager.Get(id)
		if err != nil {
			continue
		}
		if current.Metadata == nil || current.Metadata.Labels[startKeyLabel] != req.StartKey {
			return nil, status.Error(codes.FailedPrecondition, "sandbox belongs to a different start attempt")
		}
		cleanup, err := h.recoveredStartCleanup(id, current.Metadata)
		if err != nil {
			return nil, err
		}
		h.setStartCleanup(req.StartKey, cleanup)
		if err := h.cleanupStart(ctx, cleanup); err != nil {
			return nil, err
		}
		h.setStartCleanup(req.StartKey, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
 h.setStartCleanup(req.StartKey, nil)
 return acknowledgeStartFence(ctx, instance)
}

func acknowledgeStartFence(ctx context.Context, instance string) (*runtime.DeleteResponse, error) {
 if err := ctx.Err(); err != nil { return nil, status.FromContextError(err).Err() }
	// The acknowledgement prevents an older server that ignores start_key from
	// being mistaken for a server that fenced an unknown attempt.
	if err := grpc.SetHeader(ctx, metadata.Pairs("sandboxd-start-fenced", "true", "sandboxd-start-fence-instance", instance, "sandboxd-start-fence-version", "3")); err != nil {
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
 if op == terminalStartOperation { return }
	op.cleanup = cleanup
	if cleanup == nil {
		if h.startKeysByID[op.sandboxID] == key {
			delete(h.startKeysByID, op.sandboxID)
		}
		op.sandboxID = ""
		op.pending = nil
  h.compactStartOperationLocked(key, op)
	} else {
		op.sandboxID = cleanup.id
		h.startKeysByID[cleanup.id] = key
	}
}

// Snapshot only immutable pending records while completion is excluded. The
// caller filters and copies them after releasing startMu. Clearing op.pending
// or compacting its operation never changes an already retained record.
func (h *sandboxService) pendingInventoryReferencesLocked() []*runtime.SandboxStatus {
 result := make([]*runtime.SandboxStatus, 0, len(h.startKeysByID))
 for _, key := range h.startKeysByID {
  op := h.startOperations[key]
  if (op.cancel != nil || op.cleanup != nil) && op.pending != nil { result = append(result, op.pending) }
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
	key := h.startKeysByID[id]
	if op := h.startOperations[key]; op != nil && (op.cancel != nil || op.cleanup != nil) && op.sandboxID == id {
		return key
	}
	return ""
}
