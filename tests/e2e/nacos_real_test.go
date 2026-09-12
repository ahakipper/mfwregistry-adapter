//go:build nacos_real
// +build nacos_real

package e2e

import (
	"os"
	"strings"
	"testing"
	"time"

	"spotter/pkg/nacos"
)

// TestNacosReal is the opt-in real-Nacos gate named by the remediation plan.
// It intentionally skips without NACOS_SERVER; a skip is not production
// evidence. The target must be Nacos 2.x with the gRPC port (server port +
// 1000) reachable from the test host.
func TestNacosReal(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("NACOS_SERVER"))
	if address == "" {
		t.Skip("NACOS_SERVER is not configured; real Nacos evidence is required separately")
	}
	addresses := make([]string, 0)
	for _, item := range strings.Split(address, ",") {
		if item = strings.TrimSpace(item); item != "" {
			addresses = append(addresses, item)
		}
	}
	clientConfig := nacos.ClientConfig{
		TransportMode: nacos.TransportSDK,
		ServerURLs:    addresses,
		NamespaceID:   os.Getenv("NACOS_NAMESPACE"),
		GroupName:     os.Getenv("NACOS_GROUP"),
		Username:      os.Getenv("NACOS_USERNAME"),
		Password:      os.Getenv("NACOS_PASSWORD"),
		Timeout:       10 * time.Second,
	}
	if err := nacos.CheckReadinessWithConfig(clientConfig, nil); err != nil {
		t.Fatalf("readiness read+write gate: %v", err)
	}
	client, err := nacos.NewClientWithConfig(clientConfig, nil)
	if err != nil {
		t.Fatalf("create official SDK client: %v", err)
	}
	defer func() { _ = client.Close() }()
	service := "__spotter_real_" + time.Now().UTC().Format("20060102T150405.000000000")
	params := nacos.InstanceParams{ServiceName: service, IP: "127.0.0.1", Port: 1, ClusterName: "spotter-real", Enabled: true, Ephemeral: false}
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("official SDK persistent register: %v", err)
	}
	if _, err := client.ListCatalogInstances(service, "spotter-real"); err != nil {
		t.Fatalf("catalog compatibility query: %v", err)
	}
	if err := client.DeregisterInstance(params); err != nil {
		t.Fatalf("official SDK persistent deregister: %v", err)
	}
}
