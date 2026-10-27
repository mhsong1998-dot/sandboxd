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
	op := &startOperation{done: make(chan struct{}), cancel: cancel}
	h.startOperations[key] = op
	return child, func() {
		cancel()
		h.startMu.Lock()
		op.cancel = nil
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
		op = &startOperation{done: make(chan struct{})}
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
	// Metadata persists the generated identity across a sandboxd restart. Earlier
	// handlers cannot still run after restart, but their committed sandbox may exist.
	ids := make(map[string]struct{})
	h.startMu.Lock()
	if op.sandboxID != "" {
		ids[op.sandboxID] = struct{}{}
	}
	h.startMu.Unlock()
	for _, s := range h.sandboxManager.List(sandbox.ListFilterByLabels(map[string]string{startKeyLabel: req.StartKey})) {
		if s != nil && s.Metadata != nil {
			ids[s.Metadata.ID] = struct{}{}
		}
	}
	for id := range ids {
		// A caller-provided physical ID may have been reused after rollback.
		// Only the persisted key proves this sandbox belongs to this attempt.
		if current, err := h.sandboxManager.Get(id); err == nil {
			if current.Metadata == nil || current.Metadata.Labels[startKeyLabel] != req.StartKey {
				return nil, status.Error(codes.FailedPrecondition, "sandbox belongs to a different start attempt")
			}
		}
		if err := h.deleteSandbox(ctx, id); err != nil {
			return nil, err
		}
	}
	// The acknowledgement prevents an older server that ignores start_key from
	// being mistaken for a server that fenced an unknown attempt.
	if err := grpc.SetHeader(ctx, metadata.Pairs("sandboxd-start-fenced", "true")); err != nil {
		return nil, status.Error(codes.Internal, "cannot acknowledge start fence")
	}
	return &runtime.DeleteResponse{}, nil
}

// Ordinary deletion can forget a completed successful operation. Fenced keys
// remain terminal so delayed Start requests cannot recreate the execution.
func (h *sandboxService) forgetSettledStart(id string) {
	h.startMu.Lock()
	defer h.startMu.Unlock()
	for key, op := range h.startOperations {
		if op.sandboxID == id && op.cancel == nil && !op.fenced {
			delete(h.startOperations, key)
		}
	}
}
