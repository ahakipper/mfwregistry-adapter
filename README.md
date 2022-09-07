## 项目说明

发现中心的 Instance 数据，其中，容器 Instance 数据，主要来源于此。

## 构建方式

#### 本地构建二进制

```
# 构建 mac 格式
make OS=darwin

# 构建 Linux 格式
make OS=linux
```

#### 正式构建

正式构建，采用的是 docker 镜像构建方式，只需要在 gitlab 上，针对目标分支，打 tag 即可。

构建过程：https://drone-pub.mfwdev.com/PaaS/mfwregistry-adapter （私有界面，切勿修改相关内容，页面后续会对接 CAS 登录）

构建结束后，会生成镜像

> 举例来说，打的 tag 为 v0.0.1 则生成的镜像为：hub.mfwdev.com/paas/mfwregistry-adapter:v0.0.1

## 部署方式

地址：https://wiki.mafengwo.cn/pages/viewpage.action?pageId=63422398

## 使用方式

#### 使用

1、查看支持参数
```
./mfwregistry-adapter -h
```
2、运行 k8s adapter 
```
./mfwregistry-adapter adapter
```
3、默认情况下，其连接的是 config/kubeconfigs 里定义的所有 K8s 集群（多 K8s 集群支持）。
默认情况下，它会将 K8s 的数据，转换为 Instance 数据后，推送到目标发现中心。

#### 关于环境

目标发现中心，可能有多个环境，可以通过参数查看

```
./mfwregistry-adapter adapter -h
```

不同的环境，对接的发现中心，以及 etcd 集群不同。etcd 集群主要用于成员 master 选举。只有 master 节点
才可以连接 K8s 并推送数据到发现中心，其他节点属于 backup 状态。一旦 master 挂掉，其他节点会迅速补上。
（节点挂掉后，大概 10s 内完成重新选举）

## 特别提示

此项目在 Watch K8s 集群时，必须等待所有集群就绪，才能接收 Watch 的增量消息。

也就是说，如果有 3 个 K8s 集群，mfwregistry-adapter 要等所有集群都能连通，才会继续下一步。

这可能导致如下问题：

1. 如果有的集群被销毁了，无法连通了，那么一旦 mfwregistry-adapter 服务重启，则因为有集群无法连通，而整体阻塞，无法接收任何 K8s 的增量消息。

因此，为了防止这个问题发生，一旦有集群变化，必须同步更新到 mfwregistry-adapter 项目中，并在线上重启服务。

那么，为什么要这么设计？

核心的点在于，mfwregistry-adapter 是所有的实例汇总。在全量推送到 Atlas 时，会拿到所有实例，比对，同步到 Atlas 中。

然而，如果有集群无法连通。mfwregistry-adapter 自身实例就不完整。如果此时把不完全的实例与 Atlas 对比并推送，就会导致 Atlas 的数据不完整。

比如：

1. mfwregistry-adapter 有 A、B、C 集群的所有实例数据，并已经同步到 Atlas。
2. A 集群可能因为网络问题，mfwregistry-adapter 【暂时】无法连通此集群。
3. mfwregistry-adapter 重启了。如果此时，认为 B + C 是所有实例。则在往 Atlas 推送时，会将 A 集群实例，全部标记为下线。
4. Atlas 标识 A 集群的实例全部下线。A 集群的实例将全部无法通过网关、Java SDK 访问。

## 项目特性

##### 已支持特性：

* 全量推送：支持定期全量推送数据到发现中心。
* 增量推送：K8s 容器实例变化，增量通知发现中心。
* 故障恢复：如果部署多个实例，支持节点选举及故障恢复，只有 master 节点才可以推送数据，其他节点 backup。
            节点本身是可灵活增减的。
* 日志双写：控制台和日志文件均记录。

## 功能变更历史

* 2022-09-07 去除老的预发布集群 deck
* 2022-08-02 增加生产 tke 集群 otter，增加新预发布集群 slug
* 2022-04-22 主要更改consul server地址
* 2022-04-21 移除开发环境链接 hull 集群，hull 集群将下架
* 2022-02-21 防止因consul链接失败，导致死循环
* 2022-01-24 增加 sailor 集群、vipper 集群
* 2022-01-14 主要修复：全量推送逻辑里没有获取远端registry status 为 2 的实例，导致这类实例存在，却无法被更新删除（update status 为3）
* 2021-05-11 增加 consul 机器部署实例支持
* 2021-06-11 增加 K8s 调试集群 boat 支持
* 2021-06-24 k8s instance status 支持离线状态值、state 枚举值完善（如：probing、running 等）
* 2021-07-20 consul instance status、state 字段值完善（同上）