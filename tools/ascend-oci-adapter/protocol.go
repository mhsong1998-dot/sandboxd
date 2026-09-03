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

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	schemaVersion   = 1
	providerVersion = "mindcluster-ee074e93"
)

var productToken = regexp.MustCompile(`[^a-z0-9_-]+`)

type productSpec struct {
	Generation     string
	RuntimeFamily  string
	ResourceFamily string
}

var supportedProducts = map[string]productSpec{
	"ascend310p3":    {Generation: "310P", RuntimeFamily: "Ascend310P", ResourceFamily: "huawei.com/Ascend310P"},
	"ascend910b1":    {Generation: "A2", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910b2":    {Generation: "A2", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910b2c":   {Generation: "A2", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910b3":    {Generation: "A2", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910b4":    {Generation: "A2", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910b4-1":  {Generation: "A2", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910_9391": {Generation: "A3", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910_9381": {Generation: "A3", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910_9372": {Generation: "A3", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910_9392": {Generation: "A3", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910_9382": {Generation: "A3", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
	"ascend910_9362": {Generation: "A3", RuntimeFamily: "Ascend910", ResourceFamily: "huawei.com/Ascend910"},
}

type versionResponse struct {
	SchemaVersion   int    `json:"schema_version"`
	ProviderVersion string `json:"provider_version"`
}

type discoveryResponse struct {
	SchemaVersion   int      `json:"schema_version"`
	ProviderVersion string   `json:"provider_version"`
	Devices         []device `json:"devices"`
}

type device struct {
	SchedulerID    uint32 `json:"scheduler_id"`
	LogicID        int32  `json:"logic_id"`
	PhysicalID     int32  `json:"physical_id"`
	StableID       string `json:"stable_id"`
	ProductModel   string `json:"product_model"`
	Generation     string `json:"generation"`
	RuntimeFamily  string `json:"runtime_family"`
	ResourceFamily string `json:"resource_family"`
	RawProduct     string `json:"raw_product"`
	Healthy        bool   `json:"healthy"`
}

type editsRequest struct {
	SchemaVersion int     `json:"schema_version"`
	LogicIDs      []int32 `json:"logic_ids"`
	RuntimeFamily string  `json:"runtime_family"`
	PhysicalOnly  bool    `json:"physical_only"`
	MountProfile  string  `json:"mount_profile"`
}

type deviceEdit struct {
	HostPath      string `json:"host_path"`
	ContainerPath string `json:"container_path"`
	Type          string `json:"type"`
	Major         int64  `json:"major"`
	Minor         int64  `json:"minor"`
	Permissions   string `json:"permissions"`
}

type mountEdit struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Type        string   `json:"type"`
	Options     []string `json:"options"`
}

type editsResponse struct {
	SchemaVersion   int               `json:"schema_version"`
	ProviderVersion string            `json:"provider_version"`
	Devices         []deviceEdit      `json:"devices"`
	SharedDevices   []deviceEdit      `json:"shared_devices"`
	Mounts          []mountEdit       `json:"mounts"`
	Env             map[string]string `json:"env"`
}

func normalizeProductModel(raw string) (string, productSpec, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	normalized = productToken.ReplaceAllString(normalized, "")
	if !strings.HasPrefix(normalized, "ascend") {
		normalized = "ascend" + normalized
	}
	spec, ok := supportedProducts[normalized]
	if !ok {
		return "", productSpec{}, fmt.Errorf("unsupported Ascend product %q", raw)
	}
	return normalized, spec, nil
}
