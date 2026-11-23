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

import "time"

const fsStateWriteBatchSize = 16

type fsMutation func() (undo func(), err error)

type fsStateWrite struct {
	mutate    fsMutation
	submitted time.Time
	done      chan struct{}
	result    fsStateWriteResult
}

type fsStateWriteResult struct {
	wait            time.Duration
	write           time.Duration
	err             error
	failedToPersist bool
	batchSize       int
}

func (m *fsManager) writeFSState(mutate fsMutation, persist bool) fsStateWriteResult {
	op := &fsStateWrite{mutate: mutate, submitted: time.Now()}
	if !m.groupWrites || m.store == nil || !persist {
		m.applyFSStateWrites([]*fsStateWrite{op}, persist)
		return op.result
	}

	op.done = make(chan struct{})
	m.writeQueueMu.Lock()
	m.writeQueue = append(m.writeQueue, op)
	if !m.flushInProgress {
		m.flushInProgress = true
		go m.flushFSStateWrites()
	}
	m.writeQueueMu.Unlock()
	<-op.done
	return op.result
}

func (m *fsManager) flushFSStateWrites() {
	var batch [fsStateWriteBatchSize]*fsStateWrite
	for {
		m.writeQueueMu.Lock()
		if len(m.writeQueue) == 0 {
			m.flushInProgress = false
			m.writeQueueMu.Unlock()
			return
		}
		count := copy(batch[:], m.writeQueue)
		clear(m.writeQueue[:count])
		m.writeQueue = m.writeQueue[count:]
		m.writeQueueMu.Unlock()

		m.applyFSStateWrites(batch[:count], true)
		for _, op := range batch[:count] {
			close(op.done)
		}
	}
}

// All writes in a batch share one durable snapshot. Rollbacks run in reverse
// order so multiple updates for the same ID see the pre-batch state on error.
func (m *fsManager) applyFSStateWrites(batch []*fsStateWrite, persist bool) {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	start := time.Now()
	var rollback [fsStateWriteBatchSize]func()
	writes := 0
	m.mu.Lock()
	for i, op := range batch {
		op.result.wait = start.Sub(op.submitted)
		rollback[i], op.result.err = op.mutate()
		if rollback[i] != nil {
			writes++
		}
	}
	m.mu.Unlock()

	if writes == 0 || !persist {
		return
	}
	start = time.Now()
	writeErr := m.persistCurrentState()
	cost := time.Since(start)
	if writeErr != nil {
		m.mu.Lock()
		for i := len(batch) - 1; i >= 0; i-- {
			if rollback[i] != nil {
				rollback[i]()
				batch[i].result.err = writeErr
				batch[i].result.failedToPersist = true
			}
		}
		m.mu.Unlock()
	}
	for _, op := range batch {
		op.result.write = cost
		op.result.batchSize = writes
	}
}
