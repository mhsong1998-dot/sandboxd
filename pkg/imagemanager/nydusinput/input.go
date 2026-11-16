// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

// Package nydusinput validates caller-prepared, storage-independent Nydus input.
package nydusinput

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/pkg/imagemanager/imageconfig"
	"google.golang.org/protobuf/proto"
)

var digest = regexp.MustCompile(`^[a-f0-9]{64}$`)

func absolute(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/" && !strings.ContainsRune(path, 0)
}

// ValidateMetadata checks the complete input without accessing caller-owned files.
func ValidateMetadata(s *runtime.PreparedNydusSource) error {
	if s == nil || !strings.HasPrefix(s.ImageID, "sha256:") || !digest.MatchString(strings.TrimPrefix(s.ImageID, "sha256:")) {
		return fmt.Errorf("prepared Nydus requires a pinned image identity")
	}
	if !absolute(s.BootstrapPath) || !digest.MatchString(s.BootstrapSha256) {
		return fmt.Errorf("prepared Nydus requires an absolute bootstrap path and SHA256")
	}
	if s.Localfs == nil || (!absolute(s.Localfs.Dir) && !absolute(s.Localfs.BlobFile)) {
		return fmt.Errorf("prepared Nydus requires a localfs directory or file")
	}
	if s.Localfs.Dir != "" && !absolute(s.Localfs.Dir) {
		return fmt.Errorf("localfs directory must be absolute and clean")
	}
	if s.Localfs.BlobFile != "" && !absolute(s.Localfs.BlobFile) {
		return fmt.Errorf("localfs blob_file must be absolute and clean")
	}
	for _, p := range s.Localfs.AltDirs {
		if !absolute(p) {
			return fmt.Errorf("localfs alternative directory must be absolute and clean")
		}
	}
	if s.ImageConfig == nil || s.ImageConfig.Process == nil {
		return fmt.Errorf("prepared Nydus requires resolved image config and process metadata")
	}
	for _, e := range s.ImageConfig.Env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) != 2 || parts[0] == "" || strings.ContainsRune(e, 0) {
			return fmt.Errorf("prepared Nydus contains invalid image environment")
		}
	}
	p := s.ImageConfig.Process
	for _, v := range append(append(append([]string{}, p.Entrypoint...), p.Cmd...), p.Cwd, p.User) {
		if strings.ContainsRune(v, 0) {
			return fmt.Errorf("prepared Nydus contains invalid process metadata")
		}
	}
	return nil
}

// Validate checks input metadata and the current bootstrap and localfs paths.
func Validate(s *runtime.PreparedNydusSource) error {
	if err := ValidateMetadata(s); err != nil {
		return err
	}
	f, err := os.Open(s.BootstrapPath)
	if err != nil {
		return fmt.Errorf("open prepared Nydus bootstrap: %w", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("prepared Nydus bootstrap must be a regular file")
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("hash prepared Nydus bootstrap: %w", err)
	}
	if fmt.Sprintf("%x", h.Sum(nil)) != s.BootstrapSha256 {
		return fmt.Errorf("prepared Nydus bootstrap SHA256 mismatch")
	}
	for _, p := range append([]string{s.Localfs.Dir}, s.Localfs.AltDirs...) {
		if p == "" {
			continue
		}
		st, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("stat localfs directory: %w", err)
		}
		if !st.IsDir() {
			return fmt.Errorf("localfs directory is not a directory")
		}
	}
	if s.Localfs.BlobFile != "" {
		st, err := os.Stat(s.Localfs.BlobFile)
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() {
			return fmt.Errorf("localfs blob_file must be a regular file")
		}
	}
	return nil
}

// Identity binds all immutable input fields; it never relies on a mutable URL.
func Identity(s *runtime.PreparedNydusSource) (string, error) {
	if err := ValidateMetadata(s); err != nil {
		return "", err
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(s)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}

// Process returns an owned copy of the resolved image process defaults.
func Process(s *runtime.PreparedNydusSource) *imageconfig.Process {
	p := s.ImageConfig.Process
	return &imageconfig.Process{Entrypoint: append([]string(nil), p.Entrypoint...), Cmd: append([]string(nil), p.Cmd...), Cwd: p.Cwd, User: p.User}
}
