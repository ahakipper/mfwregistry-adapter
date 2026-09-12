//go:build nacos_sdk_eval
// +build nacos_sdk_eval

package e2e

import (
	"os"
	"testing"
	"time"

	"spotter/pkg/nacos"
)

// TestNacosSDKPersistentLifecycle is an opt-in scratch/pre-production gate.
// Set NACOS_SERVER to one or more comma-separated Nacos 2.x addresses and
// NACOS_NAMESPACE/NACOS_GROUP/NACOS_USERNAME/NACOS_PASSWORD as needed. The
// default CI suite intentionally skips this test when no target is supplied;
// a skip is NOT production evidence.
func TestNacosSDKPersistentLifecycle(t *testing.T) {
	address := os.Getenv("NACOS_SERVER")
	if address == "" {
		t.Skip("NACOS_SERVER is not configured; scratch Nacos SDK evidence is required separately")
	}
	var servers []string
	for _, item := range splitComma(address) {
		if item != "" {
			servers = append(servers, item)
		}
	}
	if len(servers) == 0 {
		t.Skip("NACOS_SERVER contains no addresses")
	}
	client, err := nacos.NewClientWithConfig(nacos.ClientConfig{
		TransportMode: nacos.TransportSDK,
		ServerURLs:    servers,
		NamespaceID:   os.Getenv("NACOS_NAMESPACE"),
		GroupName:     os.Getenv("NACOS_GROUP"),
		Username:      os.Getenv("NACOS_USERNAME"),
		Password:      os.Getenv("NACOS_PASSWORD"),
		Timeout:       10 * time.Second,
	}, nil)
	if err != nil {
		t.Fatalf("create SDK client: %v", err)
	}
	defer func() { _ = client.Close() }()
	service := "__spotter_sdk_eval_" + time.Now().UTC().Format("20060102T150405.000000000")
	params := nacos.InstanceParams{ServiceName: service, IP: "127.0.0.1", Port: 1, ClusterName: "spotter-sdk-eval", Enabled: true, Ephemeral: false}
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("SDK persistent register: %v", err)
	}
	if _, err := client.ListInstances(service); err != nil {
		t.Fatalf("SDK SelectAll query: %v", err)
	}
	if _, err := client.ListServices(100); err != nil {
		t.Fatalf("SDK service list: %v", err)
	}
	if err := client.Subscribe(service, os.Getenv("NACOS_GROUP"), nil, func([]nacos.Host, error) {}); err != nil {
		t.Fatalf("SDK subscribe: %v", err)
	}
	if err := client.Unsubscribe(service, os.Getenv("NACOS_GROUP"), nil, func([]nacos.Host, error) {}); err != nil {
		t.Fatalf("SDK unsubscribe: %v", err)
	}
	if err := client.DeregisterInstance(params); err != nil {
		t.Fatalf("SDK persistent deregister: %v", err)
	}
}

func splitComma(value string) []string {
	result := make([]string, 0)
	start := 0
	for i := 0; i <= len(value); i++ {
		if i != len(value) && value[i] != ',' {
			continue
		}
		item := value[start:i]
		for len(item) > 0 && (item[0] == ' ' || item[0] == '\t') {
			item = item[1:]
		}
		for len(item) > 0 && (item[len(item)-1] == ' ' || item[len(item)-1] == '\t') {
			item = item[:len(item)-1]
		}
		result = append(result, item)
		start = i + 1
	}
	return result
}
