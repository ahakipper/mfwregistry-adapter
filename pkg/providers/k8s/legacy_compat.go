package k8s

import (
	v1 "k8s.io/api/core/v1"
	legacycompat "spotter/internal/infra/legacycompat"
	sv "spotter/pkg/beehive/service/v2"
	k8srobot "spotter/pkg/k8srobot"
)

func formatInstance(obj *k8srobot.QueueObject, pod *v1.Pod) *sv.Instance {
	return formatInstanceWithDeps(obj, pod, legacycompat.PushAppCodes(), legacycompat.Logger())
}
