package mfwregistry

import (
	"github.com/k0kubun/pp"
	sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
	"testing"
)

func TestClient_Sync(t *testing.T) {
	config.GrpcAddr = "172.18.27.2:50051"
	log.LoggerInit()
	client, err := NewInstance()
	if err != nil {
		t.Error(err)
	}
	instances := []*sv.Instance{&sv.Instance{
		AppCode:    "test-test",
		InstanceId: "350461-atlasprovider-msp-98ff7cd8-rd2j9",
	}}
	r, err := client.Sync(instances)
	if err != nil {
		t.Error(err.Error())
	}
	pp.Println(r)
}
