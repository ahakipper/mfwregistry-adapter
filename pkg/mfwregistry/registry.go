package mfwregistry

import (
    "errors"
    v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
)

type Pusher interface {
    Push(triggerTime int64, instance *v2.Instance) (err error)
}

type MFWRegistry struct {
}

func NewMFWRegistry() *MFWRegistry {
    return &MFWRegistry{}
}

func (mr *MFWRegistry) Push(triggerTime int64, instance *v2.Instance) (err error) {
    // simulate push failure
    err = errors.New("the registery not exists now")

    return
}
