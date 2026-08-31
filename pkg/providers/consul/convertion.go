package consul

import (
    "encoding/json"
    "fmt"
    "github.com/hashicorp/consul/api"
    "github.com/pkg/errors"
    sv "github.com/ahakipper/spotter/pkg/beehive/service/v2"
    "github.com/ahakipper/spotter/pkg/providers"
)

const (
    metaPorts      = "ports"
    metaEnvType    = "envType"
    metaEnvGroup   = "envGroup"
    metaAppcode    = "appCode"
    metaVersion    = "version"
    metaInstanceId = "instanceId"
    metaNamespace  = "namespace"
)

const (
    ConsulHealthCheckPassing = "passing"
)

type consulInstancePort struct {
    sv.PortInfo
    // The scheme field is for compatibility, and its function is the same as protocol.
    // In the instance meta information registered by the Ecs deployment service, the field used in the port part is 'schema' instead of 'protocol'.
    Scheme string `json:"scheme"`
}

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
    // state
    var state string
    if state, err = convertState(endpoint); err != nil {
        err = errors.WithMessage(err, "convert state")
        return nil, err
    }
    // instanceId
    var instanceId string
    if instanceId, err = convertInstanceId(endpoint); err != nil {
        err = errors.WithMessage(err, "convert instance")
        return nil, err
    }
    // idc
    var idc string
    if envType == providers.EnvDev {
        idc = "office"
    } else {
        idc = "mix"
    }
    // status
    var status int32
    if status, err = convertSatus(endpoint); err != nil {
        err = errors.WithMessage(err, "convert status")
        return nil, err
    }
    // instance
    ins = &sv.Instance{
        InstanceId:  instanceId,
        Level:       "",
        Ports:       ports,
        Ip:          endpoint.Node.Address,
        EnvCode:     envCode,
        EnvType:     envType,
        EnvGroup:    envGroup,
        Cluster:     "",
        Version:     version,
        Enabled:     true,
        State:       state,
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
        Idc:         idc,
        Reversion:   int64(endpoint.Service.ModifyIndex),
        Status:      status,
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
    if san, exist := out[providers.InstanceSpringApplicationName]; exist {
        out[providers.InstanceCompatibilityLabelEnvSan] = san
    }
    // for namespace
    out[providers.InstanceCompatibilityLabelAosNamespace] = ""
    if ns, ok := out[metaNamespace]; ok {
        out[providers.InstanceCompatibilityLabelAosNamespace] = ns
    }
    // for specific label
    out[providers.InstanceCompatibilityLabelAosApp] = ""
    if lapp, exist := out[metaAppcode]; exist {
        out[providers.InstanceCompatibilityLabelAosApp] = lapp
    }
    // for destination rule and virtual service
    var drHost string
    // in case of Ecs deploy
    drHost = out[providers.InstanceCompatibilityLabelAosApp] + "." + out[providers.InstanceCompatibilityLabelAosNamespace]
    out[providers.InstanceCompatibilityLabelAosDrHost] = drHost

    return out
}

func convertPort(endpoint *api.ServiceEntry) (ports []*sv.PortInfo, err error) {
    if endpoint == nil || endpoint.Service.Meta[metaPorts] == "" {
        return nil, errors.New("convert port with none params")
    }
    ports = []*sv.PortInfo{}
    var ps []*consulInstancePort
    if err = json.Unmarshal([]byte(endpoint.Service.Meta[metaPorts]), &ps); err != nil {
        return nil, errors.WithMessage(err, "unmarshal json")
    } else {
        for idx, p := range ps {
            var protocol = p.Protocol
            if protocol == "" {
                protocol = p.Scheme
            }
            var portName = p.Name
            if portName == "" {
                portName = fmt.Sprintf(protocol+"%d", idx)
            }
            ports = append(ports, &sv.PortInfo{
                Name:        portName,
                Protocol:    protocol,
                Port:        p.Port,
                ServicePort: p.Port,
            })
        }
    }

    return ports, nil
}

func convertEnv(endpoint *api.ServiceEntry) (envType, envGroup string, err error) {
    if endpoint == nil || endpoint.Service == nil || endpoint.Service.Meta == nil {
        return "", "", errors.New("convert env with invalid endpoint")
    }
    envType = endpoint.Service.Meta[metaEnvType]
    envGroup = endpoint.Service.Meta[metaEnvGroup]

    return envType, envGroup, nil
}

func convertInstanceId(endpoint *api.ServiceEntry) (instanceId string, err error) {
    if endpoint == nil || endpoint.Service == nil || endpoint.Service.Meta == nil {
        return "", errors.New("convert instanceId with invalid endpoint")
    }
    instanceId = endpoint.Service.Meta[metaInstanceId]

    return
}

func convertAppcode(endpoint *api.ServiceEntry) (appcode string, err error) {
    if endpoint == nil || endpoint.Service == nil || endpoint.Service.Meta[metaAppcode] == "" {
        return "", errors.New("convert appcode with invalid endpoint")
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

func convertState(endpoint *api.ServiceEntry) (state string, err error) {
    state = providers.InstanceStateProbing
    if endpoint != nil {
        if len(endpoint.Checks) > 0 {
            var appcode string
            if appcode, err = convertAppcode(endpoint); err != nil {
                state = providers.InstanceStateUnknown
            } else {
                serviceCheckId := "service:" + appcode
                for _, ck := range endpoint.Checks {
                    if ck.CheckID == serviceCheckId {
                        if ck.Status == ConsulHealthCheckPassing {
                            state = providers.InstanceStateRunning
                            break
                        }
                    }
                }
            }
        }
    } else {
        state = providers.InstanceStateUnknown
        err = errors.New("convert state with invalid endpoint")
    }

    return state, nil
}

func convertSatus(endpoint *api.ServiceEntry) (status int32, err error) {
    status = providers.InstanceStatusUnhealthy
    if endpoint != nil {
        if len(endpoint.Checks) > 0 {
            var appcode string
            if appcode, err = convertAppcode(endpoint); err == nil {
                serviceCheckId := "service:" + appcode
                for _, ck := range endpoint.Checks {
                    if ck.CheckID == serviceCheckId {
                        if ck.Status == ConsulHealthCheckPassing {
                            status = providers.InstanceStatusOnline
                            break
                        }
                    }
                }
            }
        }
    } else {
        err = errors.New("convert state with invalid endpoint")
    }

    return status, nil
}
