//go:build nacos_real || nacos_sdk_eval

package e2e

import (
	"fmt"
	"spotter/pkg/nacos"
	"time"
)

// verifyNacosCanary polls a fresh SDK client until the canary disappears.
func verifyNacosCanary(client *nacos.Client, service string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		hosts, err := client.ListInstances(service)
		if err != nil {
			return err
		}
		if len(hosts) == 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("canary %s remains after bounded cleanup", service)
}
