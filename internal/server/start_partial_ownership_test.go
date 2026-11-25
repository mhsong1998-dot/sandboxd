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
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/config"
	imageapi "github.com/inclusionAI/sandboxd/pkg/imagemanager/api"
	svc "github.com/inclusionAI/sandboxd/pkg/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cancelAfterRootfsMount struct {
	*failedRootfsRelease
	cancel context.CancelFunc
}

func (s *cancelAfterRootfsMount) MountOCI(ctx context.Context, req *imageapi.OCIMountRequest) (*imageapi.OCIMountResponse, error) {
	response, err := s.fsTestImageService.MountOCI(ctx, req)
	if err == nil {
		s.cancel()
	}
	return response, err
}

func TestDeleteStartKeyRetainsRootfsAfterPreparationCancellation(t *testing.T) {
	handler := &cancelStartHandler{Handler: svc.NewFakeRuntimeHandler()}
	s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: handler})
	useFakeStartResources(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	images := &cancelAfterRootfsMount{failedRootfsRelease: &failedRootfsRelease{fsTestImageService: newFSTestImageService(), fail: true}, cancel: cancel}
	s.fsMgr = newFSManager(images)
	req := fsTestStartRequest("sbox-canceled-acquired-rootfs")
	req.StartKey, req.Runtime, req.Mounts = "canceled-acquired-rootfs", config.RuntimeNameRunsc, nil
	if _, err := s.Start(ctx, req); status.Code(err) != codes.Canceled {
		t.Fatalf("mount must succeed before preparation observes cancellation: %v", err)
	}
	if images.mountCalls(req.Rootfs.GetImageUrl()) != 1 || handler.starts.Load() != 0 {
		t.Fatal("test did not cancel between successful mount and native start")
	}
	stream := &fenceHeaderStream{}
	deleteCtx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
	if _, err := s.Delete(deleteCtx, &runtime.DeleteRequest{StartKey: req.StartKey}); err == nil {
		t.Error("preparation rollback lost its failed rootfs cleanup and acknowledged")
	}
	if len(stream.headers.Get("sandboxd-start-fenced")) != 0 {
		t.Error("failed preparation cleanup sent ACK")
	}
	if len(s.fsMgr.rootfs.List()) != 1 {
		t.Error("failed preparation cleanup discarded its rootfs owner")
	}
	if _, err := s.sandboxManager.ReserveID(req.SandboxID); err == nil {
		t.Error("failed preparation cleanup released ID")
		s.sandboxManager.ReleaseID(req.SandboxID)
	}
	images.fail = false
	if _, err := s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: req.StartKey}); err != nil {
		t.Fatal(err)
	}
	if images.releases != 3 || len(s.fsMgr.rootfs.List()) != 0 {
		t.Fatalf("cleanup retry did not release the retained owner: releases=%d owners=%d", images.releases, len(s.fsMgr.rootfs.List()))
	}
	if _, err := s.sandboxManager.ReserveID(req.SandboxID); err != nil {
		t.Fatalf("successful cleanup did not release ID: %v", err)
	}
	s.sandboxManager.ReleaseID(req.SandboxID)
}

type partialNativeStart struct {
	*failedRollbackHandler
	root string
}

func (h *partialNativeStart) Start(ctx context.Context, cfg svc.StartConfig) error {
	if err := h.failedRollbackHandler.Start(ctx, cfg); err != nil {
		return err
	}
	if err := os.MkdirAll(h.root, 0755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(h.root, "native-owner"), []byte(cfg.ID), 0600); err != nil {
		return err
	}
	return errors.New("injected partial native start failure")
}

func TestDeleteStartKeyRetainsRootAfterPartialNativeStartFailure(t *testing.T) {
	handler := &partialNativeStart{failedRollbackHandler: &failedRollbackHandler{FakeRuntimeHandler: svc.NewFakeRuntimeHandler(), failDelete: true}}
	s := newTestService(t, map[string]svc.Handler{config.RuntimeNameRunsc: handler})
	useFakeStartResources(t, s)
	const id = "sbox-partial-native-start"
	const key = "partial-native-start"
	handler.root = filepath.Join(s.config.RootDir, "containers", id)
	_, err := s.Start(context.Background(), &runtime.StartRequest{
		SandboxID: id, StartKey: key, Runtime: config.RuntimeNameRunsc,
		Rootfs: &runtime.RootfsConfig{Type: runtime.RootfsSrcType_LOCAL, Source: &runtime.RootfsConfig_Path{Path: t.TempDir()}},
	})
	if err == nil || !strings.Contains(err.Error(), "injected partial native start failure") {
		t.Fatalf("partial native start failure not reached: %v", err)
	}
	if handler.native != id || handler.deletes != 1 {
		t.Fatal("partial native must remain after failed rollback deletion")
	}
	marker := filepath.Join(handler.root, "native-owner")
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("native root was removed before successful Delete: %v", err)
	}
	stream := &fenceHeaderStream{}
	deleteCtx := grpc.NewContextWithServerTransportStream(context.Background(), stream)
	if _, err := s.Delete(deleteCtx, &runtime.DeleteRequest{StartKey: key}); err == nil {
		t.Fatal("failed partial native cleanup was acknowledged")
	}
	if len(stream.headers.Get("sandboxd-start-fenced")) != 0 {
		t.Fatal("failed partial native cleanup sent ACK")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Errorf("retry removed native root before successful Delete: %v", err)
	}
	handler.failDelete = false
	if _, err := s.Delete(fenceContext(), &runtime.DeleteRequest{StartKey: key}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(handler.root); !os.IsNotExist(err) {
		t.Fatalf("confirmed cleanup retained native root: %v", err)
	}
	if handler.native != "" || handler.deletes != 3 {
		t.Fatalf("partial native was not retired: native=%s deletes=%d", handler.native, handler.deletes)
	}
}
