/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// makeContainers builds n containers whose metadata mirrors real Kubernetes
// containers observed on a local-up-cluster (4 labels, 5-6 annotations,
// SHA-256 image refs, UUID pod IDs).  Each container is ~800 bytes when
// serialised with proto.Size.
func makeContainers(n int) []*runtime.Container {
	containers := make([]*runtime.Container, n)
	states := []runtime.ContainerState{
		runtime.ContainerState_CONTAINER_CREATED,
		runtime.ContainerState_CONTAINER_RUNNING,
		runtime.ContainerState_CONTAINER_EXITED,
	}
	// Two realistic image refs; containers alternate between them.
	imageRefs := []string{
		"sha256:e784f4560448b14a66f55c26e1b4dad2c2877cc73d001b7cd0b18e24a700a070",
		"sha256:b116e155074440ffd9e449559433feb4cd2341eb3554b1da1c638c976e56451d",
	}
	containerNames := []string{"web", "sidecar", "app", "log-collector", "nginx"}
	namespaces := []string{"default", "kube-system", "monitoring", "ingress-nginx"}

	for i := range containers {
		name := containerNames[i%len(containerNames)]
		ns := namespaces[i%len(namespaces)]
		imgRef := imageRefs[i%len(imageRefs)]
		podUID := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", i/3, i%0xFFFF, (i*7)%0xFFFF, (i*13)%0xFFFF, i*31)

		annotations := map[string]string{
			"io.kubernetes.container.hash":                     fmt.Sprintf("%08x", i),
			"io.kubernetes.container.restartCount":             "0",
			"io.kubernetes.container.terminationMessagePath":   "/dev/termination-log",
			"io.kubernetes.container.terminationMessagePolicy": "File",
			"io.kubernetes.pod.terminationGracePeriod":         "30",
		}
		// ~20% of containers have a ports annotation (like web containers).
		if i%5 == 0 {
			annotations["io.kubernetes.container.ports"] = `[{"containerPort":80,"protocol":"TCP"}]`
		}

		containers[i] = &runtime.Container{
			Id:           fmt.Sprintf("%064x", i),
			PodSandboxId: fmt.Sprintf("%064x", i/3),
			Metadata: &runtime.ContainerMetadata{
				Name:    name,
				Attempt: uint32(i % 3),
			},
			Image:     &runtime.ImageSpec{Image: imgRef},
			ImageRef:  imgRef,
			ImageId:   imgRef,
			State:     states[i%len(states)],
			CreatedAt: 1700000000000000000,
			Labels: map[string]string{
				"io.kubernetes.container.name": name,
				"io.kubernetes.pod.name":       fmt.Sprintf("%s-deploy-%010x", name, i/3),
				"io.kubernetes.pod.namespace":  ns,
				"io.kubernetes.pod.uid":        podUID,
			},
			Annotations: annotations,
		}
	}
	return containers
}

func BenchmarkBatchingStrategy(b *testing.B) {
	itemCounts := []int{5000, 10000, 25000, 50000}

	for _, count := range itemCounts {
		containers := makeContainers(count)

		if count == itemCounts[0] {
			b.Logf("proto.Size of one container: %d bytes", proto.Size(containers[0]))
		}

		b.Run(fmt.Sprintf("FixedCount/items=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			var batchCount int
			for i := 0; i < b.N; i++ {
				batchCount = 0
				_ = sendInBatches(context.Background(), containers, defaultStreamBatchSize, func(batch []*runtime.Container) error {
					batchCount++
					return nil
				})
			}
			b.ReportMetric(float64(batchCount), "batches/op")
		})

		b.Run(fmt.Sprintf("ByteSize/items=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			sizeFn := func(c *runtime.Container) int { return proto.Size(c) }
			var batchCount int
			for i := 0; i < b.N; i++ {
				batchCount = 0
				_ = sendInBatchesBySize(context.Background(), containers, defaultStreamBatchMaxBytes, sizeFn, func(batch []*runtime.Container) error {
					batchCount++
					return nil
				})
			}
			b.ReportMetric(float64(batchCount), "batches/op")
		})
	}
}

func BenchmarkProtoSize(b *testing.B) {
	containers := makeContainers(1)
	c := containers[0]
	b.ReportAllocs()
	b.ResetTimer()
	var s int
	for i := 0; i < b.N; i++ {
		s = proto.Size(c)
	}
	b.ReportMetric(float64(s), "bytes/item")
}

func TestBatchesBySize_RespectsBudget(t *testing.T) {
	containers := makeContainers(10000)
	maxBytes := defaultStreamBatchMaxBytes
	sizeFn := func(c *runtime.Container) int { return proto.Size(c) }

	_ = sendInBatchesBySize(context.Background(), containers, maxBytes, sizeFn, func(batch []*runtime.Container) error {
		var total int
		for _, c := range batch {
			total += proto.Size(c)
		}
		require.LessOrEqual(t, total, maxBytes, "batch exceeds budget")
		return nil
	})
}
