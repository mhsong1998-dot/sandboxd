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
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func Test_nameOfMethod(t *testing.T) {
	type args struct {
		fullMethod string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test1",
			args: args{
				fullMethod: "/runtime.v1.RuntimeService/CreateContainer",
			},
			want: "CreateContainer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nameOfMethod(tt.args.fullMethod); got != tt.want {
				t.Errorf("nameOfMethod() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInterceptorPreservesIncomingTraceID(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer provider.Shutdown(context.Background())
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	parent := "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", parent))
	_, err := InjectTraceInterceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/runtime.v1.SandboxService/Start"}, func(ctx context.Context, _ interface{}) (interface{}, error) {
		if got := GetTraceIdFromContext(ctx).String(); got != "0123456789abcdef0123456789abcdef" {
			t.Errorf("trace ID = %s", got)
		}
		if got := GetSpanIdFromContext(ctx).String(); got == "0123456789abcdef" {
			t.Error("server span reused the parent span ID")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
