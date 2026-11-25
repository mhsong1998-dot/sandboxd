// Copyright (c) 2026 Ant Group Corporation.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/pkg/imagemanager"
	"github.com/inclusionAI/sandboxd/pkg/resourcemanager"
	svc "github.com/inclusionAI/sandboxd/pkg/runtime"
	"github.com/inclusionAI/sandboxd/pkg/volumemanager"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHealthSnapshotFailureNamesLocalReadinessGates(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prepare  func(*sandboxService)
		expected []string
	}{
		{name: "recovery", prepare: func(s *sandboxService) { s.recoveryReady.Store(false) },
			expected: []string{"recovery_ready=false", "manager_ready=true", "resource=disabled", "image=disabled", "volume=disabled"}},
		{name: "housekeeping", prepare: func(s *sandboxService) { s.ready.Store(false) },
			expected: []string{"recovery_ready=true", "manager_ready=false", "resource=disabled", "image=disabled", "volume=disabled"}},
		{name: "resource", prepare: func(s *sandboxService) { s.resourceMod = &resourcemanager.Module{} },
			expected: []string{"recovery_ready=true", "manager_ready=true", "resource=unhealthy", "image=disabled", "volume=disabled"}},
		{name: "image", prepare: func(s *sandboxService) { s.imageMod = &imagemanager.Module{} },
			expected: []string{"recovery_ready=true", "manager_ready=true", "resource=disabled", "image=unhealthy", "volume=disabled"}},
		{name: "volume", prepare: func(s *sandboxService) { s.volumeMgr = &volumemanager.Module{} },
			expected: []string{"recovery_ready=true", "manager_ready=true", "resource=disabled", "image=disabled", "volume=unhealthy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &sandboxService{serviceHandler: cmap.New[svc.Handler]()}
			s.ready.Store(true)
			s.recoveryReady.Store(true)
			tc.prepare(s)
			require.False(t, s.Healthy())
			require.False(t, s.Ready())
			_, err := s.ListAvailableRuntimes(context.Background(), &runtime.ListAvailableRuntimesRequest{})
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.ErrorContains(t, err, "sandbox service is not ready")
			for _, expected := range tc.expected {
				require.ErrorContains(t, err, expected)
			}
		})
	}
}

func TestHealthSnapshotDisabledModulesPreserveAllocationFreeReadiness(t *testing.T) {
	s := &sandboxService{serviceHandler: cmap.New[svc.Handler]()}
	s.ready.Store(true)
	s.recoveryReady.Store(true)
	require.True(t, s.Healthy(), "unconstructed modules do not restrict readiness")
	require.True(t, s.Ready())
	response, err := s.ListAvailableRuntimes(context.Background(), &runtime.ListAvailableRuntimesRequest{})
	require.NoError(t, err)
	require.Empty(t, response.RuntimeClasses)
	require.Zero(t, testing.AllocsPerRun(100, func() { _ = s.Healthy() }), "health polling must not construct diagnostic strings")
}
