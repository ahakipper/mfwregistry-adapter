# spotter

## Project Description

The Instance data of the discovery center — among which the container Instance data mainly comes from this project.

## How to Build

#### Building the binary locally

```
# Build the mac format
make OS=darwin

# Build the Linux format
make OS=linux
```

#### Official build

The official build uses a docker image build approach: you only need to tag the target branch on gitlab.

The build process is managed by the internal CI system (private page; do not modify the related content. The page will be connected to CAS login later.)

When the build is finished, an image is generated

> For example, if the tag you pushed is v0.0.1, then the generated image is: hub.mfwdev.com/paas/spotter:v0.0.1

## How to Deploy

Address: https://wiki.mafengwo.cn/pages/viewpage.action?pageId=63422398

## How to Use

#### Usage

1. Show the supported parameters
```
./spotter -h
```
2. Run the k8s adapter
```
./spotter adapter
```
3. By default, it connects to all the K8s clusters defined in config/kubeconfigs (multiple K8s cluster support).
By default, it converts the K8s data into Instance data and pushes it to the target discovery center.

#### About environments

The target discovery center may have multiple environments, which can be viewed via parameters

```
./spotter adapter -h
```

Different environments connect to different discovery centers and etcd clusters. The etcd cluster is mainly used for member master election. Only the master node
can connect to K8s and push data to the discovery center; the other nodes are in backup state. Once the master dies, the other nodes quickly take over.
(after a node dies, re-election completes within about 10 seconds)

## Special Notes

When this project watches K8s clusters, it must wait for all clusters to be ready before it can receive the incremental Watch messages.

That is to say, if there are 3 K8s clusters, spotter has to wait until all clusters are reachable before it proceeds to the next step.

This may cause the following problem:

1. If some cluster has been destroyed and can no longer be reached, then once the spotter service restarts, it will block as a whole because that cluster is unreachable, and it cannot receive any incremental K8s messages.

Therefore, to prevent this problem, whenever there is a cluster change it must be synchronized into the spotter project, and the service must be restarted online.

So, why is it designed this way?

The core point is that spotter is the aggregation of all instances. When doing a full push to Atlas, it fetches all the instances, compares them, and syncs them into Atlas.

However, if some cluster is unreachable, the instances of spotter itself are incomplete. If at that moment the incomplete instances are compared with Atlas and pushed, the data in Atlas would become incomplete.

For example:

1. spotter has all the instance data of clusters A, B and C, and has already synced them to Atlas.
2. Cluster A may be temporarily unreachable by spotter due to a network problem.
3. spotter restarted. If at that moment B + C are considered to be all the instances, then when pushing to Atlas, all the instances of cluster A would be marked as offline.
4. Atlas marks all the instances of cluster A as offline. All the instances of cluster A would become inaccessible through the gateway and the Java SDK.

## Project Features

##### Supported features:

* Full push: supports periodically pushing all data to the discovery center.
* Incremental push: when K8s container instances change, the discovery center is notified incrementally.
* Failure recovery: if multiple instances are deployed, node election and failure recovery are supported; only the master node can push data, the other nodes are backup.
            The nodes themselves can be added and removed flexibly.
* Dual log writing: logs are written to both the console and log files.

## Change History

* 2022-10-19 Added the exception notification mechanism; added the big-monolith pre-release bernuda cluster
* 2022-09-07 Removed the old pre-release cluster deck
* 2022-08-02 Added the production tke cluster otter; added the new pre-release cluster slug
* 2022-04-22 Mainly changed the consul server address
* 2022-04-21 Removed the connection to the dev environment hull cluster; the hull cluster will be decommissioned
* 2022-02-21 Prevent an infinite loop caused by a consul connection failure
* 2022-01-24 Added the sailor cluster and the vipper cluster
* 2022-01-14 Main fix: in the full-push logic, the instances whose remote registry status was 2 were not fetched, so such instances existed but could never be updated/deleted (update status to 3)
* 2021-05-11 Added support for consul machine deployment instances
* 2021-06-11 Added the K8s debug cluster boat support
* 2021-06-24 k8s instance status supports the offline status value; the state enum values were completed (e.g. probing, running, etc.)
* 2021-07-20 consul instance status and state field values completed (same as above)
