// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/config"
	imageapi "github.com/inclusionAI/sandboxd/pkg/imagemanager/api"
	"github.com/inclusionAI/sandboxd/pkg/imagemanager/imageconfig"
	svc "github.com/inclusionAI/sandboxd/pkg/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"testing"
)

type preparedFSTestService struct {
	*fsTestImageService
	mounts, unmounts int
}

func (s *preparedFSTestService) MountPreparedNydus(_ context.Context, input *runtime.PreparedNydusSource) (*imageapi.MountInfo, error) {
	s.mounts++
	return &imageapi.MountInfo{MountPoint: "/prepared/rootfs", Env: input.ImageConfig.Env, ImageProcess: &imageconfig.Process{Cmd: input.ImageConfig.Process.Cmd}}, nil
}
func (s *preparedFSTestService) UmountPreparedNydus(*runtime.PreparedNydusSource) error {
	s.unmounts++
	return nil
}

func TestPreparedNydusRPCSourceSharesMountAndReleasesLastConsumer(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "bootstrap")
	data := []byte("bootstrap")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	input := &runtime.PreparedNydusSource{ImageID: "sha256:" + fmt.Sprintf("%064x", 1), BootstrapPath: path, BootstrapSha256: fmt.Sprintf("%x", sha256.Sum256(data)), Localfs: &runtime.LocalFsConfig{Dir: root}, ImageConfig: &runtime.NydusImageConfig{Env: []string{"FROM_IMAGE=yes"}, Process: &runtime.NydusImageProcess{Cmd: []string{"python3"}}}}
	service := &preparedFSTestService{fsTestImageService: newFSTestImageService()}
	manager := newFSManager(service)
	for _, id := range []string{"first", "second"} {
		request := &runtime.StartRequest{SandboxID: id, Rootfs: &runtime.RootfsConfig{Type: runtime.RootfsSrcType_NYDUS, Source: &runtime.RootfsConfig_Nydus{Nydus: proto.Clone(input).(*runtime.PreparedNydusSource)}}}
		prepared, err := manager.Prepare(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		if got := prepared.rootfs.RootFS.Env(); len(got) != 1 || got[0] != "FROM_IMAGE=yes" {
			t.Fatalf("lost image environment: %v", got)
		}
		state, err := stateFromPrepared(prepared)
		if err != nil {
			t.Fatal(err)
		}
		restored := &runtime.RootfsConfig{}
		if err := proto.Unmarshal(state.Rootfs, restored); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(restored.GetNydus(), input) {
			t.Fatal("input lost in filesystem state")
		}
		if err := manager.Commit(id, prepared); err != nil {
			t.Fatal(err)
		}
	}
	if service.mounts != 1 || len(service.ociMountCalls) != 0 {
		t.Fatalf("wrong mount path: prepared=%d OCI=%v", service.mounts, service.ociMountCalls)
	}
	if err := manager.Release("first"); err != nil {
		t.Fatal(err)
	}
	if service.unmounts != 0 {
		t.Fatal("unmounted while another consumer was active")
	}
	if err := manager.Release("second"); err != nil {
		t.Fatal(err)
	}
	if service.unmounts != 1 {
		t.Fatalf("unmounts=%d", service.unmounts)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("caller-owned bootstrap deleted")
	}
}

func TestStartRejectsPreparedNydusOnUnqualifiedRuntime(t *testing.T) {
	s := newTestService(t, map[string]svc.Handler{config.RuntimeNameKata: svc.NewFakeRuntimeHandler()})
	_, err := s.Start(context.Background(), &runtime.StartRequest{Runtime: config.RuntimeNameKata, Rootfs: &runtime.RootfsConfig{Type: runtime.RootfsSrcType_NYDUS}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unqualified runtime accepted: %v", err)
	}
}
