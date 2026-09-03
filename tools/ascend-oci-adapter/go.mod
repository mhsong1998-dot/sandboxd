module github.com/inclusionAI/sandboxd/tools/ascend-oci-adapter

go 1.21

require (
	ascend-common v0.0.0
	tags.cncf.io/container-device-interface/specs-go v1.0.0
)

require (
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/opencontainers/runtime-spec v1.1.0 // indirect
	github.com/opencontainers/runtime-tools v0.9.1-0.20221107090550-2e043c6bd626 // indirect
	github.com/syndtr/gocapability v0.0.0-20200815063812-42c35b437635 // indirect
	golang.org/x/mod v0.19.0 // indirect
	golang.org/x/sys v0.19.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/apimachinery v0.26.2 // indirect
	k8s.io/utils v0.0.0-20230220204549-a5ecb0141aa5 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
	tags.cncf.io/container-device-interface v1.0.0 // indirect
)

replace ascend-common => ../../third_party/mind-cluster/component/ascend-common
