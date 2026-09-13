package nacos

import (
	"fmt"
	"reflect"
	"testing"

	"spotter/internal/domain/instance"
)

func TestSplitPersistentBatchesGroupsByApplicationScopeAndOperation(t *testing.T) {
	items := []*instance.Instance{
		{InstanceId: "pay-k8s-online", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "pay-k8s-unhealthy", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusUnhealthy},
		{InstanceId: "pay-ecs-online", AppCode: "pay", Provider: "ecs", Status: instance.InstanceStatusOnline},
		{InstanceId: "pay-k8s-offline", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOffline},
	}

	batches := splitPersistentBatches(items, MaxPersistentBatchSize)
	if got, want := len(batches), 3; got != want {
		t.Fatalf("batch count = %d, want %d", got, want)
	}

	if got := batchSizes(batches); !reflect.DeepEqual(got, []int{2, 1, 1}) {
		t.Fatalf("batch sizes = %v, want [2 1 1]", got)
	}
	wantKeys := []persistentBatchKey{
		{Service: "pay", Cluster: "k8s", Operation: batchRegister},
		{Service: "pay", Cluster: "ecs", Operation: batchRegister},
		{Service: "pay", Cluster: "k8s", Operation: batchDeregister},
	}
	for i, want := range wantKeys {
		if got := batches[i].Key; got != want {
			t.Errorf("batch[%d] key = %+v, want %+v", i, got, want)
		}
	}
	wantIDs := [][]string{
		{"pay-k8s-online", "pay-k8s-unhealthy"},
		{"pay-ecs-online"},
		{"pay-k8s-offline"},
	}
	for i, want := range wantIDs {
		if got := instanceIDs(batches[i].Items); !reflect.DeepEqual(got, want) {
			t.Errorf("batch[%d] instance IDs = %v, want %v", i, got, want)
		}
	}
}

func TestSplitPersistentBatchesHardCapsEachApplicationAt100(t *testing.T) {
	items := make([]*instance.Instance, 201)
	for i := range items {
		items[i] = &instance.Instance{
			InstanceId: fmt.Sprintf("pay-k8s-%03d", i),
			AppCode:    "pay",
			Provider:   "k8s",
			Status:     instance.InstanceStatusOnline,
		}
	}

	batches := splitPersistentBatches(items, MaxPersistentBatchSize)
	if got, want := batchSizes(batches), []int{100, 100, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batch sizes = %v, want %v", got, want)
	}
}

func TestSplitPersistentBatchesPreservesStableInputOrder(t *testing.T) {
	items := []*instance.Instance{
		{InstanceId: "first", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "second", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "third", AppCode: "other", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "fourth", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
	}

	batches := splitPersistentBatches(items, MaxPersistentBatchSize)
	if got, want := len(batches), 2; got != want {
		t.Fatalf("batch count = %d, want %d", got, want)
	}
	if got := instanceIDs(batches[0].Items); !reflect.DeepEqual(got, []string{"first", "second", "fourth"}) {
		t.Fatalf("first application order = %v, want [first second fourth]", got)
	}
	if got := instanceIDs(batches[1].Items); !reflect.DeepEqual(got, []string{"third"}) {
		t.Fatalf("second application order = %v, want [third]", got)
	}
}

func batchSizes(batches []persistentBatch) []int {
	sizes := make([]int, len(batches))
	for i, batch := range batches {
		sizes[i] = len(batch.Items)
	}
	return sizes
}

func instanceIDs(items []*instance.Instance) []string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.InstanceId
	}
	return ids
}
