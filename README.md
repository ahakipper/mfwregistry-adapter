## 项目说明

发现中心的 Instance 数据，其中，容器 Instance 数据，主要来源于此。

## 构建方式

```
# 构建 mac 格式
make OS=darwin

# 构建 Linux 格式
make OS=linux
```

## 使用方式

#### 使用

1、查看支持参数
```
./mfwregistry-k8sadapter -h
```
2、运行 k8s adapter 
```
./mfwregistry-k8sadapter k8sadapter
```
3、默认情况下，其连接的是 config/kubeconfigs 里定义的所有 K8s 集群（多 K8s 集群支持）。
默认情况下，它会将 K8s 的数据，转换为 Instance 数据后，推送到目标发现中心。

#### 关于环境

目标发现中心，可能有多个环境，可以通过参数查看

```
./mfwregistry-k8sadapter k8sadapter -h
```

不同的环境，对接的发现中心，以及 etcd 集群不同。etcd 集群主要用于成员 master 选举。只有 master 节点
才可以连接 K8s 并推送数据到发现中心，其他节点属于 backup 状态。一旦 master 挂掉，其他节点会迅速补上。
（节点挂掉后，大概 10s 内完成重新选举）