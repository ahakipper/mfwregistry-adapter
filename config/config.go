package config

var (
    EtcdEndpoints  = []string{} // etcd members
    CertFile       string       // etcd cert file
    KeyFile        string       // etcd key file
    CAFile         string       // etcd ca file
    KubeConfigPath []string     // K8s kubeconfig files path
    // log
    LogFilePath string // log file path
    LogLevel    int    // log level
    LogBackups  int    // log back numbers
    LogSize     int    // log size
    LogAge      int    // log age
    LogEncoding string // log encoding, log, or json
    LogToStd    bool   // log to std
    // push
    PushAllInterval int // The time interval of full push（seconds）
    // grpc
    GrpcAddr string
    // DisablePushWorker will stop the real push action of the worker but only print push info. This configuration is for test use only.
    DisablePushWorker bool
    // Providers
    Providers []string
    // ConsulAddress is the consul server address
    ConsulAddress []string
    // LockCampaignKey Indicates the prefix key for participating in the campaign, store in etcd
    LockCampaignKey string
    PushAppCodes    []string
    // EnableLeaderElection Leader election
    EnableLeaderElection bool
)

func InitTest() {
    EtcdEndpoints = []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"}
    CertFile = "./config/certs/etcdtest/etcd.pem"
    KeyFile = "./config/certs/etcdtest/etcd-key.pem"
    CAFile = "./config/certs/etcdtest/ca.pem"
    KubeConfigPath = []string{"./config/kubeconfigs/k8s-hull", "./config/kubeconfigs/k8s-boat"}
    ConsulAddress = []string{"10.72.73.172:8520", "10.72.73.173:8520", "10.72.73.174:8520"}
    Providers = []string{}
    LockCampaignKey = "/paas/mfwregistry-adapter-test"
}

func InitDev() {
    EtcdEndpoints = []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"}
    CertFile = "./config/certs/etcdtest/etcd.pem"
    KeyFile = "./config/certs/etcdtest/etcd-key.pem"
    CAFile = "./config/certs/etcdtest/ca.pem"
    KubeConfigPath = []string{"./config/kubeconfigs/k8s-hull", "./config/kubeconfigs/k8s-boat", "./config/kubeconfigs/k8s-sailor", "./config/kubeconfigs/k8s-vipper"}
    ConsulAddress = []string{"10.72.73.172:8520", "10.72.73.173:8520", "10.72.73.174:8520"}
    Providers = []string{}
    LockCampaignKey = "/paas/mfwregistry-adapter"
}

func InitProd() {
    EtcdEndpoints = []string{"192.168.11.100:2479", "192.168.11.101:2479", "192.168.11.102:2479"}
    // this dir depend on Dockerfile
    CertFile = "./config/certs/etcdprod/etcd.pem"
    KeyFile = "./config/certs/etcdprod/etcd-key.pem"
    CAFile = "./config/certs/etcdprod/ca.pem"
    KubeConfigPath = []string{"./config/kubeconfigs/k8s-deck", "./config/kubeconfigs/k8s-eel"}
    ConsulAddress = []string{"10.132.2.40:8520", "10.132.2.42:8520", "10.132.2.43:8520"}
    Providers = []string{}
    LockCampaignKey = "/paas/mfwregistry-adapter"
}
