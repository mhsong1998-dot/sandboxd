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

package trace

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/status"
)

const slowDelete = 100 * time.Millisecond

var (
	deleteTimingEnabled  = os.Getenv("SANDBOXD_DELETE_TIMING") == "1"
	activeDeleteRequests atomic.Int64
	activeDeleteCleanups atomic.Int64
)

type deleteTimingKey struct{}

type DeleteRequestTiming struct {
	traceID oteltrace.TraceID
	started time.Time
	active  int64
}

type DeleteCleanupTiming struct {
	traceID oteltrace.TraceID
	started time.Time
	ended   time.Time
	active  int64
	stages  map[string]time.Duration
}

type DeleteStage struct {
	cleanup *DeleteCleanupTiming
	started time.Time
}

func BeginDeleteRequest(ctx context.Context) *DeleteRequestTiming {
	if !deleteTimingEnabled {
		return nil
	}
	return &DeleteRequestTiming{
		traceID: GetTraceIdFromContext(ctx),
		started: time.Now(),
		active:  activeDeleteRequests.Add(1),
	}
}

func BeginDeleteCleanup(ctx context.Context, request *DeleteRequestTiming) (context.Context, *DeleteCleanupTiming) {
	if request == nil {
		return ctx, nil
	}
	cleanup := &DeleteCleanupTiming{
		traceID: request.traceID,
		started: time.Now(),
		active:  activeDeleteCleanups.Add(1),
		stages:  make(map[string]time.Duration, 18),
	}
	return context.WithValue(ctx, deleteTimingKey{}, cleanup), cleanup
}

func BeginDeleteStage(ctx context.Context) DeleteStage {
	if !deleteTimingEnabled {
		return DeleteStage{}
	}
	cleanup, _ := ctx.Value(deleteTimingKey{}).(*DeleteCleanupTiming)
	if cleanup == nil {
		return DeleteStage{}
	}
	return DeleteStage{cleanup: cleanup, started: time.Now()}
}

func (s DeleteStage) End(stage string) {
	if s.cleanup != nil {
		s.cleanup.stages[stage] += time.Since(s.started)
	}
}

func logDeleteTiming(id oteltrace.TraceID, elapsed time.Duration, err error) bool {
	return err != nil || elapsed >= slowDelete || id[15]&63 == 0
}

func (t *DeleteCleanupTiming) Finish(err error) {
	if t == nil {
		return
	}
	t.ended = time.Now()
	activeDeleteCleanups.Add(-1)
	elapsed := t.ended.Sub(t.started)
	if !logDeleteTiming(t.traceID, elapsed, err) {
		return
	}
	fields := logrus.Fields{
		"event":              "sandboxd_delete_cleanup",
		"trace_id":           t.traceID.String(),
		"elapsed_us":         elapsed.Microseconds(),
		"concurrent_cleanup": t.active,
		"outcome":            status.Code(err).String(),
	}
	for stage, elapsed := range t.stages {
		fields[stage+"_us"] = elapsed.Microseconds()
	}
	logrus.WithFields(fields).Info("sandboxd delete cleanup timing")
}

func (t *DeleteRequestTiming) Finish(cleanup *DeleteCleanupTiming, shared bool, err error) {
	if t == nil {
		return
	}
	now := time.Now()
	activeDeleteRequests.Add(-1)
	elapsed := now.Sub(t.started)
	if !logDeleteTiming(t.traceID, elapsed, err) {
		return
	}
	fields := logrus.Fields{
		"event":              "sandboxd_delete_request",
		"trace_id":           t.traceID.String(),
		"elapsed_us":         elapsed.Microseconds(),
		"concurrent_request": t.active,
		"shared_cleanup":     shared,
		"outcome":            status.Code(err).String(),
	}
	if cleanup != nil {
		fields["cleanup_trace_id"] = cleanup.traceID.String()
		fields["before_cleanup_us"] = max(time.Duration(0), cleanup.started.Sub(t.started)).Microseconds()
		fields["after_cleanup_us"] = now.Sub(cleanup.ended).Microseconds()
		fields["joined_running_cleanup"] = cleanup.started.Before(t.started)
	}
	logrus.WithFields(fields).Info("sandboxd delete request timing")
}
