package consul

import (
    "encoding/json"
    "github.com/hashicorp/consul/api"
    "github.com/pkg/errors"
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
)

const (
    metaPorts    = "ports"
    metaEnvType  = "envType"
    metaEnvGroup = "envGroup"
    metaAppcode  = "appCode"
    metaVersion  = "version"
)

var instanceBaseAttrMetas = map[string]struct{}{metaPorts: {}, metaEnvType: {}, metaEnvGroup: {}, metaAppcode: {}, metaVersion: {}}

// convertInstance convert a Consul valid Endpoint to a Instance.
// As the Endpoints obtained from Consul are valid and passed the health check. Therefore, when converting to Instance,
// we need to set the value of Instance  Status filed to 1.
func convertInstance(endpoint *api.CatalogService) (ins *sv.Instance, err error) {
    if endpoint == nil {
        return nil, errors.New("nil consul service endpoint")
    }
    // ports
    var ports []*sv.PortInfo
    if ports, err = convertPort(endpoint); err != nil {
        err = errors.WithMessage(err, "convert instance")
        return nil, err
    }
    // env
    var envType, envGroup string
    if envType, envGroup, err = convertEnv(endpoint); err != nil {
        err = errors.WithMessage(err, "convert instance")
        return nil, err
    }
    envCode := envType + "#" + envGroup
    // appcode
    var appcode string
    if appcode, err = convertAppcode(endpoint); err != nil {
        err = errors.WithMessage(err, "convert instance")
        return nil, err
    }
    // version
    var version string
    if version, err = convertVersion(endpoint); err != nil {
        err = errors.WithMessage(err, "convert instance")
        return nil, err
    }
    ins = &sv.Instance{
        InstanceId:  endpoint.Node,
        Level:       "",
        Ports:       ports,
        Ip:          endpoint.ServiceAddress,
        EnvCode:     envCode,
        EnvType:     envType,
        EnvGroup:    envGroup,
        Cluster:     "",
        Version:     version,
        Enabled:     true,
        State:       "",
        HealthState: "",
        AppCode:     appcode,
        Provider:    "ecs",
        Label:       convertLabels(endpoint),
        Hostname:    endpoint.Node,
        Cpu:         0,
        Memory:      0,
        Disk:        0,
        Os:          "",
        Image:       map[string]string{},
        Idc:         "",
        Reversion:   int64(endpoint.ModifyIndex),
        Status:      1,
    }

    return ins, nil
}

func convertLabels(endpoint *api.CatalogService) map[string]string {
    out := make(map[string]string)
    for key, val := range endpoint.ServiceMeta {
        if _, ok := instanceBaseAttrMetas[key]; !ok {
            out[key] = val
        }
    }

    return out
}

func convertPort(endpoint *api.CatalogService) (ports []*sv.PortInfo, err error) {
    if endpoint == nil || endpoint.ServiceMeta[metaPorts] == "" {
        return nil, errors.New("convert port with none params")
    }
    ports = []*sv.PortInfo{}
    if err = json.Unmarshal([]byte(endpoint.ServiceMeta[metaPorts]), &ports); err != nil {
        return nil, errors.WithMessage(err, "unmarshal json")
    }

    return ports, nil
}

func convertEnv(endpoint *api.CatalogService) (envType, envGroup string, err error) {
    if endpoint == nil {
        return "", "", errors.New("convert env with none endpoint")
    }
    envType = endpoint.ServiceMeta[metaEnvType]
    envGroup = endpoint.ServiceMeta[metaEnvGroup]

    return envType, envGroup, nil
}

func convertAppcode(endpoint *api.CatalogService) (appcode string, err error) {
    if endpoint == nil || endpoint.ServiceMeta[metaAppcode] == "" {
        return "", errors.New("convert appcode with none endpoint")
    }
    appcode = endpoint.ServiceMeta[metaAppcode]

    return appcode, nil
}

func convertVersion(endpoint *api.CatalogService) (appcode string, err error) {
    if endpoint == nil || endpoint.ServiceMeta[metaVersion] == "" {
        return "", errors.New("convert version with invalid params")
    }
    appcode = endpoint.ServiceMeta[metaVersion]

    return appcode, nil
}
