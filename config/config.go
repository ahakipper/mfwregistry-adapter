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
    // MetricsAddr The Prometheus metrics address
    MetricsAddr string
)

func InitTest() {
    EtcdEndpoints = []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"}
    CertFile = "./config/certs/etcdtest/etcd.pem"
    KeyFile = "./config/certs/etcdtest/etcd-key.pem"
    CAFile = "./config/certs/etcdtest/ca.pem"
    KubeConfigPath = []string{"./config/kubeconfigs/k8s-sailor"}
    ConsulAddress = []string{"10.72.73.172:8520", "10.72.73.173:8520", "10.72.73.174:8520"}
    Providers = []string{}
    LockCampaignKey = "/paas/mfwregistry-adapter-test"
}

func InitDev() {
    EtcdEndpoints = []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"}
    CertFile = "./config/certs/etcdtest/etcd.pem"
    KeyFile = "./config/certs/etcdtest/etcd-key.pem"
    CAFile = "./config/certs/etcdtest/ca.pem"
    // sailor: 微服务开发环境；vipper：大单体开发环境
    KubeConfigPath = []string{
        "./config/kubeconfigs/k8s-sailor",
        "./config/kubeconfigs/k8s-vipper",
    }
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
    // deck：微服务老预发布；eel：微服务腾仁机房；otter：微服务 TKE；slug：微服务新预发布
    KubeConfigPath = []string{
        "./config/kubeconfigs/k8s-eel",
        "./config/kubeconfigs/k8s-otter",
        "./config/kubeconfigs/k8s-slug",
        "./config/kubeconfigs/k8s-bernuda",
    }
    ConsulAddress = []string{"10.132.2.40:8520", "10.132.2.42:8520", "10.132.2.43:8520"}
    Providers = []string{}
    LockCampaignKey = "/paas/mfwregistry-adapter"
}
