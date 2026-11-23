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
	"errors"
	"testing"
	"time"

	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func TestDeleteTimingSampling(t *testing.T) {
	id := oteltrace.TraceID{15: 1}
	if logDeleteTiming(id, slowDelete-time.Nanosecond, nil) {
		t.Fatal("unexpected sample for a fast request")
	}
	if !logDeleteTiming(id, slowDelete, nil) || !logDeleteTiming(id, 0, errors.New("cleanup failed")) {
		t.Fatal("slow and failed requests must always be recorded")
	}
	id[15] = 64
	if !logDeleteTiming(id, 0, nil) {
		t.Fatal("matching fast request should be sampled")
	}
}

func TestRequestParentExtractsSingleValidTraceID(t *testing.T) {
	const traceID = "6fbddd6edbeb48b17e92d843f81d2fed"
	const child = "fccf51bf5b19a69d"
	const parent = "00-" + traceID + "-" + child + "-01"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", parent))
	id := requestParent(ctx)
	if id.TraceID().String() != traceID || id.SpanID().String() != child {
		t.Fatalf("unexpected trace parent: %v", id)
	}
	for _, values := range [][]string{nil, {"broken"}, {parent, parent}, {"00-00000000000000000000000000000000-" + child + "-01"}} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{"traceparent": values})
		if requestParent(ctx).IsValid() {
			t.Errorf("accepted invalid traceparent %q", values)
		}
	}
}
