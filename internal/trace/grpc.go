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
	"strings"
	"time"

	"github.com/inclusionAI/sandboxd/internal/metrics"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func InjectTraceInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if incoming, ok := metadata.FromIncomingContext(ctx); ok {
		carrier := propagation.MapCarrier{}
		for _, key := range []string{"traceparent", "tracestate"} {
			if values := incoming.Get(key); len(values) > 0 {
				carrier[key] = values[0]
			}
		}
		ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	}
	ctx, span := otel.Tracer("sandboxd").Start(ctx, nameOfMethod(info.FullMethod), oteltrace.WithSpanKind(oteltrace.SpanKindServer))
	defer span.End()
	traceID := span.SpanContext().TraceID().String()
	if !span.SpanContext().IsValid() {
		traceID = GetTraceIdFromContext(ctx).String()
	}
	ctx = context.WithValue(ctx, ContextKeyTraceId, traceID)
	ctx = context.WithValue(ctx, ContextKeySpanId, span.SpanContext().SpanID().String())
	logrus.WithField(ContextKeyTraceId, traceID).Debugf("received %s request, raw-request:[%+v]", info.FullMethod, req)
	start := time.Now()
	resp, err := handler(ctx, req)
	cost := time.Since(start)
	metrics.RecordActionLatencyMs(nameOfMethod(info.FullMethod), cost.Milliseconds())
	if err != nil {
		span.RecordError(err)
		metrics.RecordActionResult(nameOfMethod(info.FullMethod), "failed")
	} else {
		metrics.RecordActionResult(nameOfMethod(info.FullMethod), "success")
	}
	return resp, err
}

func nameOfMethod(fullMethod string) string {
	// fullMethod is in format "/package.Service/Method"
	// we only need the "Method" part
	return fullMethod[strings.LastIndex(fullMethod, "/")+1:]
}
