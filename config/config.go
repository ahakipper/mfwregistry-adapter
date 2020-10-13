package config

var (
    EtcdEndpoints  = []string{} // etcd 节点列表
    CertFile       string       // etcd 证书
    KeyFile        string       // etcd 证书
    CAFile         string       // etcd 证书
    KubeConfigPath string       // K8s kubeconfig 配置文件地址
)

func InitTest() {
    EtcdEndpoints = []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"}
    CertFile = "./config/certs/etcdtest/etcd.pem"
    KeyFile = "./config/certs/etcdtest/etcd-key.pem"
    CAFile = "./config/certs/etcdtest/ca.pem"
    KubeConfigPath = "./config/kubeconfigs/k8s-test"
}

func InitDev() {
    EtcdEndpoints = []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"}
    CertFile = "./config/certs/etcdtest/etcd.pem"
    KeyFile = "./config/certs/etcdtest/etcd-key.pem"
    CAFile = "./config/certs/etcdtest/ca.pem"
    KubeConfigPath = ""
}

func InitProd() {
    EtcdEndpoints = []string{"192.168.11.100:2379", "192.168.11.101:2379", "192.168.11.102:2379"}

    // this dir depend on Dockerfile
    CertFile = "/go/workspace/tools/screct/client.pem"
    KeyFile = "/go/workspace/tools/screct/client-key-pem"
    CAFile = "/go/workspace/tools/screct/ca.pem"
}
