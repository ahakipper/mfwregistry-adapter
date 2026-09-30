package consul

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/consul/api"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
	"spotter/pkg/providers"
	workerpkg "spotter/pkg/worker"
)

// ConsulSource describes one logical Consul catalog. Addresses are an HA
// endpoint list for this source; they must not be merged with another source's
// addresses. Security and tenancy options are scoped to this source and are
// applied to every Consul client created for its endpoint list.
type ConsulSource struct {
	ID        string
	Addresses []string

	Token     string
	TokenFile string

	// TLSConfig mirrors the Consul API TLS settings. The flattened fields below
	// are convenient for config descriptors and take precedence when set.
	TLSConfig             api.TLSConfig
	TLSCAFile             string
	TLSCertFile           string
	TLSKeyFile            string
	TLSServerName         string
	TLSInsecureSkipVerify bool

	Datacenter string
	Namespace  string

	// Short aliases are accepted for descriptors that do not use the TLS
	// prefix. The TLS-prefixed fields are canonical when both are supplied.
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

func (s ConsulSource) clientOptions() ConsulClientOptions {
	caFile, certFile, keyFile, serverName := s.TLSCAFile, s.TLSCertFile, s.TLSKeyFile, s.TLSServerName
	if caFile == "" {
		caFile = s.CAFile
	}
	if certFile == "" {
		certFile = s.CertFile
	}
	if keyFile == "" {
		keyFile = s.KeyFile
	}
	if serverName == "" {
		serverName = s.ServerName
	}
	options := ConsulClientOptions{
		Token:                 s.Token,
		TokenFile:             s.TokenFile,
		TLSConfig:             s.TLSConfig,
		TLSCAFile:             caFile,
		TLSCertFile:           certFile,
		TLSKeyFile:            keyFile,
		TLSServerName:         serverName,
		TLSInsecureSkipVerify: s.TLSInsecureSkipVerify || s.InsecureSkipVerify,
		Datacenter:            s.Datacenter,
		Namespace:             s.Namespace,
	}
	return options
}

// Normalized returns a value safe to pass to a provider constructor. It trims
// and de-duplicates addresses while preserving their configured order, and
// derives a stable ID when the caller leaves ID empty.
func (s ConsulSource) Normalized() (ConsulSource, error) {
	addresses := make([]string, 0, len(s.Addresses))
	seen := make(map[string]struct{}, len(s.Addresses))
	for _, address := range s.Addresses {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		addresses = append(addresses, address)
	}
	if len(addresses) == 0 {
		return ConsulSource{}, errors.New("consul source has no usable addresses")
	}
	id := strings.TrimSpace(s.ID)
	id, err := instance.ConsulSourceID(id, addresses)
	if err != nil {
		return ConsulSource{}, err
	}
	return ConsulSource{
		ID:                    id,
		Addresses:             addresses,
		Token:                 s.Token,
		TokenFile:             s.TokenFile,
		TLSConfig:             s.TLSConfig,
		TLSCAFile:             s.TLSCAFile,
		TLSCertFile:           s.TLSCertFile,
		TLSKeyFile:            s.TLSKeyFile,
		TLSServerName:         s.TLSServerName,
		TLSInsecureSkipVerify: s.TLSInsecureSkipVerify,
		Datacenter:            s.Datacenter,
		Namespace:             s.Namespace,
		CAFile:                s.CAFile,
		CertFile:              s.CertFile,
		KeyFile:               s.KeyFile,
		ServerName:            s.ServerName,
		InsecureSkipVerify:    s.InsecureSkipVerify,
	}, nil
}

// NormalizeSources validates and normalizes a complete source set. IDs are
// the source scope, so duplicate IDs are rejected before providers are built.
func NormalizeSources(sources []ConsulSource) ([]ConsulSource, error) {
	if len(sources) == 0 {
		return nil, errors.New("no consul sources configured")
	}
	normalized := make([]ConsulSource, 0, len(sources))
	seen := make(map[string]struct{}, len(sources))
	wireSeen := make(map[string]string, len(sources))
	for i, source := range sources {
		resolved, err := source.Normalized()
		if err != nil {
			return nil, fmt.Errorf("normalize consul source %d: %w", i, err)
		}
		if _, ok := seen[resolved.ID]; ok {
			return nil, fmt.Errorf("duplicate consul source ID %q", resolved.ID)
		}
		wireID := instance.SanitizeWireScope(resolved.ID)
		if wireID == "" {
			return nil, fmt.Errorf("consul source ID %q has no usable Nacos wire scope", resolved.ID)
		}
		if previous, ok := wireSeen[wireID]; ok {
			return nil, fmt.Errorf("consul source IDs %q and %q collide after Nacos wire sanitization as %q", previous, resolved.ID, wireID)
		}
		seen[resolved.ID] = struct{}{}
		wireSeen[wireID] = resolved.ID
		normalized = append(normalized, resolved)
	}
	return normalized, nil
}

type sourceProviderConstructor func(context.Context, workerpkg.Worker, int, ConsulSource, ports.Logger, ports.Notifier) (providers.Provider, error)

// NewConsulProvidersWithSources constructs one fully independent provider per
// logical source. NewConsulProviderWithSourceID creates a fresh client
// factory, monitor, cache, overflow queue, and watch lifecycle on every call.
func NewConsulProvidersWithSources(ctx context.Context, worker workerpkg.Worker, pushInterval int, sources []ConsulSource, logger ports.Logger, notifier ports.Notifier) ([]providers.Provider, error) {
	return newConsulProvidersWithConstructor(ctx, worker, pushInterval, sources, logger, notifier, func(ctx context.Context, worker workerpkg.Worker, pushInterval int, source ConsulSource, logger ports.Logger, notifier ports.Notifier) (providers.Provider, error) {
		return NewConsulProviderWithSource(ctx, worker, pushInterval, source, logger, notifier)
	})
}

func newConsulProvidersWithConstructor(ctx context.Context, worker workerpkg.Worker, pushInterval int, sources []ConsulSource, logger ports.Logger, notifier ports.Notifier, construct sourceProviderConstructor) ([]providers.Provider, error) {
	if construct == nil {
		return nil, errors.New("nil consul source provider constructor")
	}
	normalized, err := NormalizeSources(sources)
	if err != nil {
		return nil, err
	}
	result := make([]providers.Provider, 0, len(normalized))
	for _, source := range normalized {
		provider, err := construct(ctx, worker, pushInterval, source, logger, notifier)
		if err != nil {
			for _, created := range result {
				if closable, ok := created.(*consul); ok {
					closable.shutdown()
				}
			}
			return nil, fmt.Errorf("new consul provider for source %q: %w", source.ID, err)
		}
		if concrete, ok := provider.(*consul); ok {
			concrete.acceptLegacyRemote = len(normalized) == 1
		}
		result = append(result, provider)
	}
	return result, nil
}

// NewConsulProviders is the concise constructor spelling for callers that
// already have a resolved descriptor list.
func NewConsulProviders(ctx context.Context, worker workerpkg.Worker, pushInterval int, sources []ConsulSource, logger ports.Logger, notifier ports.Notifier) ([]providers.Provider, error) {
	return NewConsulProvidersWithSources(ctx, worker, pushInterval, sources, logger, notifier)
}
