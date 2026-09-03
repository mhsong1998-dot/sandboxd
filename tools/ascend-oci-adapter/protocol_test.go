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

package main

import "testing"

func TestNormalizeProductModel(t *testing.T) {
	tests := map[string]struct {
		model         string
		runtimeFamily string
	}{
		"310P3":          {model: "ascend310p3", runtimeFamily: "Ascend310P"},
		"910B4":          {model: "ascend910b4", runtimeFamily: "Ascend910"},
		"Ascend910B2C":   {model: "ascend910b2c", runtimeFamily: "Ascend910"},
		"Ascend910_9391": {model: "ascend910_9391", runtimeFamily: "Ascend910"},
	}
	for raw, expected := range tests {
		model, product, err := normalizeProductModel(raw)
		if err != nil || model != expected.model || product.RuntimeFamily != expected.runtimeFamily {
			t.Fatalf("normalize %q = %q/%q, %v; want %q/%q", raw, model, product.RuntimeFamily,
				err, expected.model, expected.runtimeFamily)
		}
	}
	if _, _, err := normalizeProductModel("Ascend910"); err == nil {
		t.Fatal("generic Ascend910 alias must be rejected")
	}
}
