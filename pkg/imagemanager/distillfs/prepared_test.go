// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

package distillfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/pkg/imagemanager/imageconfig"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"testing"
)

type forbiddenRegistry struct{ calls int }

func (f *forbiddenRegistry) FetchAndExtractBootstrapWithImageConfig(context.Context, string, string, string) (string, []string, *imageconfig.Process, error) {
	f.calls++
	return "", nil, nil, fmt.Errorf("Registry must not be called")
}

func TestPreparedDaemonPreservesBackendAndResolvedEmptyProcess(t *testing.T) {
	root := t.TempDir()
	boot := filepath.Join(root, "caller-bootstrap")
	data := []byte("bootstrap")
	if err := os.WriteFile(boot, data, 0600); err != nil {
		t.Fatal(err)
	}
	input := &runtime.PreparedNydusSource{ImageID: "sha256:" + fmt.Sprintf("%064x", 1), BootstrapPath: boot, BootstrapSha256: fmt.Sprintf("%x", sha256.Sum256(data)), Localfs: &runtime.LocalFsConfig{Dir: root, AltDirs: []string{root}}, ImageConfig: &runtime.NydusImageConfig{Process: &runtime.NydusImageProcess{}}}
	registry := &forbiddenRegistry{}
	mgr := &manager{ctx: context.Background(), root: filepath.Join(root, "manager"), nydusClient: registry, nydusCfgTemplate: BackendConfig{BackendType: "registry", Registry: &RegistryConfig{Host: "must-not-be-used"}}}
	d, err := mgr.setupNydusDaemon(&DaemonCreateOpt{ID: "prepared", SourceType: "nydus", Prepared: input})
	if err != nil {
		t.Fatal(err)
	}
	if d.config.BackendType != "localfs" || d.config.Registry != nil || d.meta.BootstrapPath != boot || !d.meta.EnvResolved || d.meta.ImageProcess == nil {
		t.Fatalf("input not propagated: %s", d.config.BackendType)
	}
	input.Localfs.AltDirs[0] = "/mutated"
	if d.config.Localfs.AltDirs[0] != root || d.meta.Prepared.Localfs.AltDirs[0] != root {
		t.Fatal("input was not copied")
	}
	if err := d.applyConfig(); err != nil {
		t.Fatal(err)
	}
	copy := d.config.DeepCopy()
	copy.Localfs.AltDirs[0] = "/changed-copy"
	if d.config.Localfs.AltDirs[0] != root {
		t.Fatal("backend DeepCopy aliases alternative directories")
	}
	if err := os.MkdirAll(filepath.Dir(d.savedPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := d.saveMeta(); err != nil {
		t.Fatal(err)
	}
	loaded := &Daemon{ctx: context.Background(), nydusClient: registry, savedPath: d.savedPath}
	if err := loaded.LoadExisted(d.savedPath); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(loaded.meta.Prepared, d.meta.Prepared) || loaded.config.Localfs.Dir != root {
		t.Fatal("prepared input lost across recovery")
	}
	// A complete persisted prepared source can repair a missing derived process
	// cache without asking the Registry, including an intentionally empty Cmd.
	loaded.meta.ImageProcess = nil
	got, err := loaded.ResolveImageProcess(context.Background())
	if err != nil || got == nil {
		t.Fatalf("process recovery: %v", err)
	}
	if registry.calls != 0 {
		t.Fatalf("unexpected Registry calls: %d", registry.calls)
	}
	if err := os.RemoveAll(d.meta.DaemonDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(boot); err != nil {
		t.Fatal("caller bootstrap was removed")
	}
}
