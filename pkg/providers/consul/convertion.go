package consul

import (
    "encoding/json"
    "github.com/hashicorp/consul/api"
    "github.com/pkg/errors"
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
)

const (
    metaPorts                 = "ports"
    metaEnvType               = "envType"
    metaEnvGroup              = "envGroup"
    metaAppcode               = "appCode"
    metaVersion               = "version"
    metaNamespace             = "namespace"
    metaSpringApplicationName = "spring.application.name"
)

var instanceBaseAttrMetas = map[string]struct{}{metaPorts: {}, metaEnvType: {}, metaEnvGroup: {}, metaAppcode: {}, metaVersion: {}, metaNamespace: {}}

// convertInstance convert a Consul valid Endpoint to a Instance.
// As the Endpoints obtained from Consul are valid and passed the health check. Therefore, when converting to Instance,
// we need to set the value of Instance  Status filed to 1.
func convertInstance(endpoint *api.ServiceEntry) (ins *sv.Instance, err error) {
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
        InstanceId:  endpoint.Node.Node,
        Level:       "",
        Ports:       ports,
        Ip:          endpoint.Node.Address,
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
        Hostname:    endpoint.Node.Node,
        Cpu:         0,
        Memory:      0,
        Disk:        0,
        Os:          "",
        Image:       map[string]string{},
        Idc:         "",
        Reversion:   int64(endpoint.Service.ModifyIndex),
        Status:      1,
    }
    return ins, nil
}

func convertLabels(endpoint *api.ServiceEntry) map[string]string {
    out := make(map[string]string)
    // Convert normal labels
    if endpoint.Service.Meta != nil {
        out = endpoint.Service.Meta
    }
    //for key, val := range endpoint.Service.Meta {
    //    if _, ok := instanceBaseAttrMetas[key]; !ok {
    //        out[key] = val
    //    }
    //}
    // for Java SDK
    if san, exist := out[metaSpringApplicationName]; exist {
        out["env:san"] = san
    }
    // for namespace
    out["compatibility:aos_namespace"] = ""
    if ns, ok := out[metaNamespace]; ok {
        out["compatibility:aos_namespace"] = ns
    }
    // for specific label
    out["compatibility:aos_app"] = ""
    if lapp, exist := out[metaAppcode]; exist {
        out["compatibility:aos_app"] = lapp
    }
    // for destination rule and virtual service
    var drHost string
    // in case of Ecs deploy
    drHost = out["compatibility:aos_app"] + "." + out["compatibility:aos_namespace"]
    out["compatibility:aos_dr_host"] = drHost

    return out
}

func convertPort(endpoint *api.ServiceEntry) (ports []*sv.PortInfo, err error) {
    if endpoint == nil || endpoint.Service.Meta[metaPorts] == "" {
        return nil, errors.New("convert port with none params")
    }
    ports = []*sv.PortInfo{}
    if err = json.Unmarshal([]byte(endpoint.Service.Meta[metaPorts]), &ports); err != nil {
        return nil, errors.WithMessage(err, "unmarshal json")
    }

    return ports, nil
}

func convertEnv(endpoint *api.ServiceEntry) (envType, envGroup string, err error) {
    if endpoint == nil {
        return "", "", errors.New("convert env with none endpoint")
    }
    envType = endpoint.Service.Meta[metaEnvType]
    envGroup = endpoint.Service.Meta[metaEnvGroup]

    return envType, envGroup, nil
}

func convertAppcode(endpoint *api.ServiceEntry) (appcode string, err error) {
    if endpoint == nil || endpoint.Service.Meta[metaAppcode] == "" {
        return "", errors.New("convert appcode with none endpoint")
    }
    appcode = endpoint.Service.Meta[metaAppcode]

    return appcode, nil
}

func convertVersion(endpoint *api.ServiceEntry) (appcode string, err error) {
    if endpoint == nil || endpoint.Service.Meta[metaVersion] == "" {
        return "", errors.New("convert version with invalid params")
    }
    appcode = endpoint.Service.Meta[metaVersion]

    return appcode, nil
}
