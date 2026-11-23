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
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	runtime "github.com/inclusionAI/sandboxd/api/runtime/v1"
	"github.com/inclusionAI/sandboxd/config"
	"github.com/inclusionAI/sandboxd/pkg/store"
	"github.com/stretchr/testify/require"
)

type blockingFSStateStore struct {
	store.DbStore
	writes       atomic.Int32
	firstEntered chan struct{}
	firstRelease chan struct{}
	failSecond   bool
}

func (s *blockingFSStateStore) StoreRaw(key string, data []byte) error {
	switch s.writes.Add(1) {
	case 1:
		close(s.firstEntered)
		<-s.firstRelease
	case 2:
		if s.failSecond {
			return errors.New("failed batch")
		}
	}
	return s.DbStore.StoreRaw(key, data)
}

func TestConcurrentFilesystemUpdatesUseOneDurableWriteAndRollBackTogether(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail=%t", fail), func(t *testing.T) {
			backing := store.NewStoreImp(t.TempDir() + "/state.db")
			db := &blockingFSStateStore{
				DbStore: backing, firstEntered: make(chan struct{}),
				firstRelease: make(chan struct{}), failSecond: fail,
			}
			m := newFSManager(nil, db)
			m.groupWrites = true
			commit := func(id string) error {
				return m.Commit(id, &preparedFS{source: &runtime.RootfsConfig{
					Type:   runtime.RootfsSrcType_LOCAL,
					Source: &runtime.RootfsConfig_Path{Path: "/local/rootfs"},
				}})
			}
			first, second, third := make(chan error, 1), make(chan error, 1), make(chan error, 1)
			go func() { first <- commit("sandbox-a") }()
			select {
			case <-db.firstEntered:
			case <-time.After(time.Second):
				t.Fatal("first write never started")
			}
			t.Cleanup(func() {
				select {
				case <-db.firstRelease:
				default:
					close(db.firstRelease)
				}
			})
			go func() { second <- m.Release("sandbox-a") }()
			go func() { third <- commit("sandbox-b") }()
			require.Eventually(t, func() bool {
				m.writeQueueMu.Lock()
				defer m.writeQueueMu.Unlock()
				return len(m.writeQueue) == 2
			}, time.Second, time.Millisecond)
			select {
			case <-second:
				t.Fatal("release acknowledged before durability")
			case <-third:
				t.Fatal("commit acknowledged before durability")
			default:
			}

			close(db.firstRelease)
			require.NoError(t, <-first)
			for _, result := range []<-chan error{second, third} {
				if fail {
					require.ErrorContains(t, <-result, "failed batch")
				} else {
					require.NoError(t, <-result)
				}
			}
			require.Equal(t, int32(2), db.writes.Load())
			data, err := backing.LoadRaw(config.SandboxFSStateBucket)
			require.NoError(t, err)
			var state storedSandboxFSStates
			require.NoError(t, json.Unmarshal(data, &state))
			expected := "sandbox-b"
			if fail {
				expected = "sandbox-a"
			}
			require.Len(t, state.Items, 1)
			require.Contains(t, state.Items, expected)
			require.Len(t, m.sandboxState, 1)
			require.Contains(t, m.sandboxState, expected)
		})
	}
}
