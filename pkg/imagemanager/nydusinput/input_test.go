// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

package nydusinput

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"google.golang.org/protobuf/proto"
)

func fixture(t *testing.T) *runtime.PreparedNydusSource {
	t.Helper()
	root := t.TempDir()
	bootstrap := filepath.Join(root, "bootstrap")
	data := []byte("prepared bootstrap")
	if err := os.WriteFile(bootstrap, data, 0600); err != nil {
		t.Fatal(err)
	}
	return &runtime.PreparedNydusSource{
		ImageID: "sha256:" + fmt.Sprintf("%064x", 1), BootstrapPath: bootstrap,
		BootstrapSha256: fmt.Sprintf("%x", sha256.Sum256(data)),
		Localfs:         &runtime.LocalFsConfig{Dir: root},
		ImageConfig:     &runtime.NydusImageConfig{Process: &runtime.NydusImageProcess{}},
	}
}

func TestPreparedInputRequiresCompleteMetadata(t *testing.T) {
	valid := fixture(t)
	if err := Validate(valid); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*runtime.PreparedNydusSource){
		"missing config":     func(s *runtime.PreparedNydusSource) { s.ImageConfig = nil },
		"unresolved process": func(s *runtime.PreparedNydusSource) { s.ImageConfig.Process = nil },
		"relative bootstrap": func(s *runtime.PreparedNydusSource) { s.BootstrapPath = "bootstrap" },
		"relative blobs":     func(s *runtime.PreparedNydusSource) { s.Localfs.Dir = "blobs" },
		"mutable identity":   func(s *runtime.PreparedNydusSource) { s.ImageID = "image:latest" },
		"wrong digest":       func(s *runtime.PreparedNydusSource) { s.BootstrapSha256 = fmt.Sprintf("%064x", 2) },
		"invalid env":        func(s *runtime.PreparedNydusSource) { s.ImageConfig.Env = []string{"not-an-assignment"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := proto.Clone(valid).(*runtime.PreparedNydusSource)
			mutate(s)
			if err := Validate(s); err == nil {
				t.Fatal("incomplete input accepted")
			}
		})
	}
}

func TestPreparedIdentityIncludesBackendAndProcess(t *testing.T) {
	s := fixture(t)
	first, err := Identity(s)
	if err != nil {
		t.Fatal(err)
	}
	clone := proto.Clone(s).(*runtime.PreparedNydusSource)
	same, err := Identity(clone)
	if err != nil || same != first {
		t.Fatalf("unstable identity: %s %v", same, err)
	}
	clone.ImageConfig.Process.Cmd = []string{"python3"}
	different, err := Identity(clone)
	if err != nil || different == first {
		t.Fatal("process defaults missing from identity")
	}
	clone = proto.Clone(s).(*runtime.PreparedNydusSource)
	clone.Localfs.AltDirs = []string{"/another-source"}
	different, err = Identity(clone)
	if err != nil || different == first {
		t.Fatal("backend missing from identity")
	}
}
