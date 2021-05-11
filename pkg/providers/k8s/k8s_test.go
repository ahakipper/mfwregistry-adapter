package k8s

import (
	"fmt"
	client "gitlab.mfwdev.com/servicemesh/robot"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestFormatInstance(t *testing.T) {
	// test for env test in New FengXiao
	obj := &client.QueueObject{
		Event: client.EventDelete,
	}
	pod := &v1.Pod{
	}
	pod.Labels = map[string]string{
		"app":               "testiterationc-msp",
		"app-code":          "testiterationc-msp",
		"cluster":           "",
		"deploy-id":         "987",
		"deploy-name":       "31-test-testiterationc-msp",
		"env-group":         "7",
		"env-type":          "test",
		"pod-template-hash": "c4dcf8b8f",
		"source-platform":   "beehive",
		"version":           "31",
	}
	pod.Name = "31-test-testiterationc-msp-c4dcf8b8f-gldvb"
	pod.Namespace = "msp"
	pod.Spec.Containers = []v1.Container{
		v1.Container{
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "APP_CODE",
					Value: "testiterationc-msp",
				},
				v1.EnvVar{
					Name:  "APP_ENV_TYPE",
					Value: "test",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_TYPE",
					Value: "dev",
				},
			},
			Ports: []v1.ContainerPort{
				v1.ContainerPort{
					Name:          "http-0",
					ContainerPort: 80,
					Protocol:      "tcp",
				},
			},
		},
	}
	instance := formatInstance(obj, pod)
	if instance.EnvType != "test" {
		t.Error(fmt.Sprintf("The value of the instance env typ does not meet expectations, appcode: %s", instance.AppCode))
		t.FailNow()
	}

	// test for env test in Old FengXiao
	obj = &client.QueueObject{
		Event: client.EventDelete,
	}
	pod = &v1.Pod{
	}
	pod.Labels = map[string]string{
		"app":               "governance-order-beta-122",
		"app-code":          "governance-order",
		"cluster":           "",
		"deploy-id":         "2038",
		"deploy-name":       "1000620-beta-governance-order",
		"env-group":         "122",
		"env-type":          "beta",
		"pod-template-hash": "665885b4cd",
		"version":           "7f41f56db0f7bee99c1255cef07c07335ecb99ef",
	}
	pod.Name = "1000620-beta-governance-order-665885b4cd-zjwwn"
	pod.Namespace = "mtech"
	pod.Spec.Containers = []v1.Container{
		v1.Container{
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "MAFENGWO_MSERVICE",
					Value: "true",
				},
				v1.EnvVar{
					Name:  "PHYSICAL_DOCKER0_IP",
					Value: "172.17.0.1",
				},
				v1.EnvVar{
					Name:  "SKIPPER_IP",
					Value: "msidecar.component",
				},
				v1.EnvVar{
					Name:  "KPROXY_IP",
					Value: "kproxy.component",
				},
				v1.EnvVar{
					Name:  "APP_NAME",
					Value: "governance-order-beta-122",
				},
				v1.EnvVar{
					Name:  "APP_NAMESPACE",
					Value: "mtech",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_TYPE",
					Value: "BETA",
				},
				v1.EnvVar{
					Name:  "SERVICE_INSTANCE_ID",
					Value: "acbca73f-f726-4511-8cd6-ac8202141cef",
				},
				v1.EnvVar{
					Name:  "SERVICE_HTTP_PORT",
					Value: "80",
				},
				v1.EnvVar{
					Name: "SERVICE_GRPC_PORT",
				},
				v1.EnvVar{
					Name:  "SERVICE_ENV_GROUP",
					Value: "122",
				},
				v1.EnvVar{
					Name:  "SERVICE_ENV_TYPE",
					Value: "beta",
				},
				v1.EnvVar{
					Name:  "SERVICE_VERSION",
					Value: "7f41f56db0f7bee99c1255cef07c07335ecb99ef",
				},
				v1.EnvVar{
					Name:  "SERVICE_CLUSTER",
					Value: "hull",
				},
			},
			Ports: []v1.ContainerPort{
				v1.ContainerPort{
					Name:          "http-0",
					ContainerPort: 80,
					Protocol:      "tcp",
				},
			},
		},
	}
	instance = formatInstance(obj, pod)
	if instance.EnvType != "beta" {
		t.Error(fmt.Sprintf("The value of the instance env typ does not meet expectations, appcode: %s", instance.AppCode))
		t.FailNow()
	}

	// test for env dev in Aos microservice
	obj = &client.QueueObject{
		Event: client.EventDelete,
	}
	pod = &v1.Pod{
	}
	pod.Labels = map[string]string{
		"app":               "oudder.mservice",
		"cadvisor-app":      "oudder-mservice",
		"env-type":          "dev",
		"microservice":      "rudder",
		"pod-template-hash": "db9cff469",
		"version":           "349672",
	}
	pod.Name = "349672-oudder-mservice-db9cff469-95k4h"
	pod.Namespace = "mservice"
	pod.Spec.Containers = []v1.Container{
		v1.Container{
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "APP_NAME",
					Value: "oudder",
				},
				v1.EnvVar{
					Name:  "APP_NAMESPACE",
					Value: "mservice",
				},
				v1.EnvVar{
					Name:  "APP_VERSION",
					Value: "0.0.877",
				},
				v1.EnvVar{
					Name:  "APP_CODE",
					Value: "oudder-mservice",
				},
				v1.EnvVar{
					Name:  "APP_ENV_TYPE",
					Value: "dev",
				},
				v1.EnvVar{
					Name:  "APP_ENV_GROUP",
					Value: "",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_TYPE",
					Value: "dev",
				},
			},
			Ports: []v1.ContainerPort{
				v1.ContainerPort{
					Name:          "http-0",
					ContainerPort: 80,
					Protocol:      "tcp",
				},
			},
		},
	}
	instance = formatInstance(obj, pod)
	if instance.EnvType != "dev" {
		t.Error(fmt.Sprintf("The value of the instance env typ does not meet expectations, appcode: %s", instance.AppCode))
		t.FailNow()
	}

	// test for env product in Aos microservice
	obj = &client.QueueObject{
		Event: client.EventDelete,
	}
	pod = &v1.Pod{
	}
	pod.Labels = map[string]string{
		"app":               "oudder.mservice",
		"cadvisor-app":      "oudder-mservice",
		"env-type":          "product",
		"microservice":      "rudder",
		"pod-template-hash": "589fd89b74",
		"version":           "352414",
	}
	pod.Name = "352414-oudder-mservice-589fd89b74-csdm2"
	pod.Namespace = "mservice"
	pod.Spec.Containers = []v1.Container{
		v1.Container{
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "MAFENGWO_MSERVICE",
					Value: "true",
				},
				v1.EnvVar{
					Name:  "PHYSICAL_DOCKER0_IP",
					Value: "172.17.0.1",
				},
				v1.EnvVar{
					Name:  "SKIPPER_IP",
					Value: "msidecar.component",
				},
				v1.EnvVar{
					Name:  "KPROXY_IP",
					Value: "kproxy.component",
				},
				v1.EnvVar{
					Name:  "APP_VERSION_ID",
					Value: "352414",
				},
				v1.EnvVar{
					Name:  "APP_NAME",
					Value: "oudder",
				},
				v1.EnvVar{
					Name:  "APP_NAMESPACE",
					Value: "mservice",
				},
				v1.EnvVar{
					Name:  "APP_VERSION",
					Value: "0.0.879",
				},
				v1.EnvVar{
					Name:  "PROJECT_ROOT",
					Value: "/opt/oudder/",
				},
				v1.EnvVar{
					Name:  "VENDOR_PATH",
					Value: "/opt/oudder/vendor",
				},
				v1.EnvVar{
					Name:  "MAResource_MYSQL_PUBLIC_DB",
					Value: "MAResource_MYSQL_PUBLIC_DB",
				},
				v1.EnvVar{
					Name:  "APP_CODE",
					Value: "oudder-mservice",
				},
				v1.EnvVar{
					Name:  "APP_IDC",
					Value: "kraken",
				},
				v1.EnvVar{
					Name:  "APP_PROVIDER",
					Value: "k8s",
				},
				v1.EnvVar{
					Name:  "APP_ENV_TYPE",
					Value: "product",
				},
				v1.EnvVar{
					Name: "APP_ENV_GROUP",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_TYPE",
					Value: "product",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_NAME",
					Value: "kraken",
				},
			},
			Ports: []v1.ContainerPort{
				v1.ContainerPort{
					Name:          "http-0",
					ContainerPort: 80,
					Protocol:      "tcp",
				},
			},
		},
	}
	instance = formatInstance(obj, pod)
	if instance.EnvType != "product" {
		t.Error(fmt.Sprintf("The value of the instance env typ does not meet expectations, appcode: %s", instance.AppCode))
		t.FailNow()
	}

	// test for env product in Aos microservice with no env-type label
	obj = &client.QueueObject{
		Event: client.EventDelete,
	}
	pod = &v1.Pod{
	}
	pod.Labels = map[string]string{
		"app":               "coeussimulation.msp",
		"cadvisor-app":      "coeussimulation-msp",
		"microservice":      "rudder",
		"pod-template-hash": "67cc579797",
		"version":           "312699",
	}
	pod.Name = "312699-coeussimulation-msp-67cc579797-trxwb"
	pod.Namespace = "msp"
	pod.Spec.Containers = []v1.Container{
		v1.Container{
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "MAFENGWO_MSERVICE",
					Value: "true",
				},
				v1.EnvVar{
					Name:  "PHYSICAL_DOCKER0_IP",
					Value: "172.17.0.1",
				},
				v1.EnvVar{
					Name:  "SKIPPER_IP",
					Value: "msidecar.component",
				},
				v1.EnvVar{
					Name:  "KPROXY_IP",
					Value: "kproxy.component",
				},
				v1.EnvVar{
					Name:  "APP_VERSION_ID",
					Value: "312699",
				},
				v1.EnvVar{
					Name:  "APP_NAME",
					Value: "coeussimulation",
				},
				v1.EnvVar{
					Name:  "APP_NAMESPACE",
					Value: "msp",
				},
				v1.EnvVar{
					Name:  "APP_VERSION",
					Value: "0.0.30",
				},
				v1.EnvVar{
					Name:  "PROJECT_ROOT",
					Value: "/srv/coeussimulation/",
				},
				v1.EnvVar{
					Name:  "VENDOR_PATH",
					Value: "/srv/coeussimulation/vendor",
				},
				v1.EnvVar{
					Name:  "MAResource_MYSQL_PUBLIC_DB",
					Value: "MAResource_MYSQL_PUBLIC_DB",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_TYPE",
					Value: "product",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_NAME",
					Value: "kraken",
				},
			},
			Ports: []v1.ContainerPort{
				v1.ContainerPort{
					Name:          "http-0",
					ContainerPort: 80,
					Protocol:      "tcp",
				},
			},
		},
	}
	instance = formatInstance(obj, pod)
	if instance.EnvType != "product" {
		t.Error(fmt.Sprintf("The value of the instance env typ does not meet expectations, appcode: %s", instance.AppCode))
		t.FailNow()
	}

	// test for env staging in Aos microservice
	obj = &client.QueueObject{
		Event: client.EventDelete,
	}
	pod = &v1.Pod{
	}
	pod.Labels = map[string]string{
		"app":               "useraccount.muser",
		"cadvisor-app":      "useraccount-muser",
		"env-group":         "10014",
		"env-type":          "staging",
		"microservice":      "rudder",
		"pod-template-hash": "848cc77b",
		"version":           "356885",
	}
	pod.Name = "356885-useraccount-muser-848cc77b-nkx8h"
	pod.Namespace = "muser"
	pod.Spec.Containers = []v1.Container{
		v1.Container{
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "MAFENGWO_MSERVICE",
					Value: "true",
				},
				v1.EnvVar{
					Name:  "PHYSICAL_DOCKER0_IP",
					Value: "172.17.0.1",
				},
				v1.EnvVar{
					Name:  "SKIPPER_IP",
					Value: "msidecar.component",
				},
				v1.EnvVar{
					Name:  "KPROXY_IP",
					Value: "kproxy.component",
				},
				v1.EnvVar{
					Name:  "APP_VERSION_ID",
					Value: "356885",
				},
				v1.EnvVar{
					Name:  "APP_NAME",
					Value: "useraccount",
				},
				v1.EnvVar{
					Name:  "APP_NAMESPACE",
					Value: "muser",
				},
				v1.EnvVar{
					Name:  "APP_VERSION",
					Value: "1.0.79",
				},
				v1.EnvVar{
					Name:  "PROJECT_ROOT",
					Value: "/srv/useraccount/",
				},
				v1.EnvVar{
					Name:  "VENDOR_PATH",
					Value: "/srv/useraccount/vendor",
				},
				v1.EnvVar{
					Name:  "MAResource_MYSQL_PUBLIC_DB",
					Value: "MAResource_MYSQL_PUBLIC_DB",
				},
				v1.EnvVar{
					Name:  "APP_CODE",
					Value: "useraccount-muser",
				},
				v1.EnvVar{
					Name:  "APP_IDC",
					Value: "deck",
				},
				v1.EnvVar{
					Name:  "APP_PROVIDER",
					Value: "k8s",
				},
				v1.EnvVar{
					Name:  "APP_ENV_TYPE",
					Value: "staging",
				},
				v1.EnvVar{
					Name:  "APP_ENV_GROUP",
					Value: "10014",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_TYPE",
					Value: "staging",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_NAME",
					Value: "deck",
				},
			},
			Ports: []v1.ContainerPort{
				v1.ContainerPort{
					Name:          "http-0",
					ContainerPort: 80,
					Protocol:      "tcp",
				},
			},
		},
	}
	instance = formatInstance(obj, pod)
	if instance.EnvType != "staging" {
		t.Error(fmt.Sprintf("The value of the instance env typ does not meet expectations, appcode: %s", instance.AppCode))
		t.FailNow()
	}

	// test for env staging in Aos microservice with no env label
	obj = &client.QueueObject{
		Event: client.EventDelete,
	}
	pod = &v1.Pod{
	}
	pod.Labels = map[string]string{
		"app":               "mgateway.msales",
		"cadvisor-app":      "mgateway-msales",
		"microservice":      "rudder",
		"pod-template-hash": "5f48578b44",
		"version":           "303840",
	}
	pod.Name = "303840-mgateway-msales-5f48578b44-dldd8"
	pod.Namespace = "msales"
	pod.Spec.Containers = []v1.Container{
		v1.Container{
			Env: []v1.EnvVar{
				v1.EnvVar{
					Name:  "MAFENGWO_MSERVICE",
					Value: "true",
				},
				v1.EnvVar{
					Name:  "PHYSICAL_DOCKER0_IP",
					Value: "172.17.0.1",
				},
				v1.EnvVar{
					Name:  "SKIPPER_IP",
					Value: "msidecar.component",
				},
				v1.EnvVar{
					Name:  "KPROXY_IP",
					Value: "kproxy.component",
				},
				v1.EnvVar{
					Name:  "APP_VERSION_ID",
					Value: "303840",
				},
				v1.EnvVar{
					Name:  "APP_NAME",
					Value: "mgateway",
				},
				v1.EnvVar{
					Name:  "APP_NAMESPACE",
					Value: "msales",
				},
				v1.EnvVar{
					Name:  "APP_VERSION",
					Value: "0.0.191",
				},
				v1.EnvVar{
					Name:  "PROJECT_ROOT",
					Value: "/srv/mgateway/",
				},
				v1.EnvVar{
					Name:  "VENDOR_PATH",
					Value: "/srv/mgateway/vendor",
				},
				v1.EnvVar{
					Name:  "MAResource_MYSQL_PUBLIC_DB",
					Value: "MAResource_MYSQL_PUBLIC_DB",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_TYPE",
					Value: "staging",
				},
				v1.EnvVar{
					Name:  "K8S_CLUSTER_NAME",
					Value: "deck",
				},
			},
			Ports: []v1.ContainerPort{
				v1.ContainerPort{
					Name:          "http-0",
					ContainerPort: 80,
					Protocol:      "tcp",
				},
			},
		},
	}
	instance = formatInstance(obj, pod)
	if instance.EnvType != "staging" {
		t.Error(fmt.Sprintf("The value of the instance env typ does not meet expectations, appcode: %s", instance.AppCode))
		t.FailNow()
	}

}
