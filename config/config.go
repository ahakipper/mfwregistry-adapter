package config

var (
    EtcdEndpoints  = []string{} // etcd members
    CertFile       string       // etcd cert file
    KeyFile        string       // etcd key file
    CAFile         string       // etcd ca file
    KubeConfigPath string       // K8s kubeconfig files path
    // log
    LogFilePath string // log file path
    LogLevel    int    // log level
    LogBackups  int    // log back numbers
    LogSize     int    // log size
    LogAge      int    // log age
    LogEncoding string // log encoding, log, or json
    LogToStd    bool   // log to std
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
