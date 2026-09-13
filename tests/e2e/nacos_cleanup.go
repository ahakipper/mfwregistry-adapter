//go:build nacos_real || nacos_sdk_eval

package e2e

import (
	"fmt"
	"spotter/pkg/nacos"
	"time"
)

// verifyNacosCanary polls a fresh SDK client until the canary disappears.
func verifyNacosCanary(cfg nacos.ClientConfig, service string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		client, err := nacos.NewClientWithConfig(cfg, nil)
		if err != nil {
			return err
		}
		hosts, err := client.ListInstances(service)
		closeErr := client.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if len(hosts) == 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("canary %s remains after bounded cleanup", service)
}
