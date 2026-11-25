// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/config"
	"github.com/inclusionAI/sandboxd/internal/util"
	svc "github.com/inclusionAI/sandboxd/pkg/runtime"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/stretchr/testify/require"
)

func TestCheckedRootRetirementPreservesHousekeepingReadiness(t *testing.T) {
	for _, tc := range []struct {
		name        string
		checkedRoot bool
		writeErr    error
		wantReady   bool
	}{
		{name: "strict root cleanup before metadata deletion", checkedRoot: true, wantReady: true},
		{name: "live object missing temporary file", wantReady: false},
		{name: "real persistence permission error after retirement", checkedRoot: true, writeErr: os.ErrPermission, wantReady: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			health := make(chan bool, 1)
			m := &Manager{root: t.TempDir(), recyclePath: t.TempDir(), sandboxes: cmap.New[*Sandbox](),
				serviceHandler: cmap.New[svc.Handler](), monitorStopChan: cmap.New[chan struct{}](),
				exitNotifiers: cmap.New[*exitNotifier](), healthChan: health,
				idGenerator: util.NewUUIDGenerator(config.SandboxPrefix, nil), maxSandboxNum: 10}
			m.serviceHandler.Set("runsc", svc.NewFakeRuntimeHandler())
			const id = "sbox-checked-root"
			_, err := m.ReserveID(id)
			require.NoError(t, err)
			root := filepath.Join(m.root, id)
			require.NoError(t, os.MkdirAll(root, 0700))
			status := &housekeepingWriteBarrier{path: filepath.Join(root, "status"),
				staged: make(chan string, 1), resume: make(chan struct{}), writeErr: tc.writeErr}
			defer close(status.resume)
			original := &Sandbox{Metadata: &api.SandboxMetadata{ID: id, RuntimeHandler: "runsc"}, PATH: root, Status: status}
			m.sandboxes.Set(id, original)
			m.monitorStopChan.Set(id, make(chan struct{}))
			done := make(chan struct{})
			go func() { m.housekeeping(); close(done) }()
			select {
			case temporary := <-status.staged:
				if tc.checkedRoot {
					// This is cleanupStart's real ordering: checked root first,
					// DeleteWithContext (which used to mark retirement) later.
					require.NoError(t, m.CleanSandboxRootChecked(id))
					require.NoError(t, os.MkdirAll(root, 0700))
					// Model RemoveAll's interval after unlinking the temporary
					// file but before removing the still-visible root.
					require.NoFileExists(t, temporary)
					require.DirExists(t, root)
					if !original.retiring.Load() {
						t.Error("checked cleanup did not retire the exact object before metadata deletion")
					}
					require.True(t, m.sandboxes.Has(id))
					require.Equal(t, 1, m.idGenerator.Len(), "checked cleanup must retain the ID")
					_, err := m.ReserveID(id)
					require.Error(t, err)
				} else {
					require.NoError(t, os.Remove(temporary))
				}
			case <-time.After(5 * time.Second):
				t.Fatal("housekeeping did not reach persistence")
			}
			status.resume <- struct{}{}
			select {
			case ready := <-health:
				require.Equal(t, tc.wantReady, ready)
			case <-time.After(5 * time.Second):
				t.Fatal("housekeeping did not complete")
			}
			<-done
			if !tc.checkedRoot {
				return
			}
			m.DeleteWithContext(context.Background(), id)
			require.False(t, m.sandboxes.Has(id))
			require.Zero(t, m.idGenerator.Len())

			// Reusing the physical ID creates a distinct live object. Its own
			// missing temporary file must still close readiness.
			_, err = m.ReserveID(id)
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(root, 0700))
			replacementStatus := &housekeepingWriteBarrier{path: filepath.Join(root, "status"),
				staged: make(chan string, 1), resume: make(chan struct{})}
			defer close(replacementStatus.resume)
			replacement := &Sandbox{Metadata: &api.SandboxMetadata{ID: id, RuntimeHandler: "runsc"}, PATH: root, Status: replacementStatus}
			m.sandboxes.Set(id, replacement)
			m.monitorStopChan.Set(id, make(chan struct{}))
			require.False(t, replacement.retiring.Load())
			done = make(chan struct{})
			go func() { m.housekeeping(); close(done) }()
			select {
			case temporary := <-replacementStatus.staged:
				require.NoError(t, os.Remove(temporary))
			case <-time.After(5 * time.Second):
				t.Fatal("replacement did not reach persistence")
			}
			replacementStatus.resume <- struct{}{}
			select {
			case ready := <-health:
				require.False(t, ready, "new object must not inherit retirement")
			case <-time.After(5 * time.Second):
				t.Fatal("replacement housekeeping did not complete")
			}
			<-done
		})
	}
}

func TestCheckedRootIOFailureRetainsReservationAndExactObject(t *testing.T) {
	// A self-referential ancestor deterministically causes ELOOP, even when
	// CCE tests run as root; chmod-based permission failures would not.
	root := filepath.Join(t.TempDir(), "blocked")
	require.NoError(t, os.Symlink("blocked", root))
	const id = "sbox-checked-io-error"
	original := &Sandbox{}
	m := &Manager{root: root, sandboxes: cmap.New[*Sandbox](),
		idGenerator: util.NewUUIDGenerator(config.SandboxPrefix, nil), maxSandboxNum: 10}
	m.sandboxes.Set(id, original)
	_, err := m.ReserveID(id)
	require.NoError(t, err)
	require.ErrorContains(t, m.CleanSandboxRootChecked(id), "clean sandbox root:")
	require.True(t, original.retiring.Load())
	require.True(t, m.sandboxes.Has(id))
	require.Equal(t, 1, m.idGenerator.Len())
	_, err = m.ReserveID(id)
	require.Error(t, err, "I/O failure must not release the physical ID")
	require.NoError(t, os.Remove(root))
	require.NoError(t, os.MkdirAll(filepath.Join(root, id), 0700))
	require.NoError(t, m.CleanSandboxRootChecked(id), "same reserved attempt remains retryable")
	require.Equal(t, 1, m.idGenerator.Len(), "only final Manager deletion releases the reservation")
}
