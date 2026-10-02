# Nacos 3.2.4 Persistent Query-Plane Root Cause

Date: 2026-10-02  
Nacos source tag: `3.2.4`  
Source commit: `2c587c04891d532df1544ae95b906b677ac8eeff`

## Conclusion

The 956–980/1000 gRPC Query result is not caused by pagination, a Go SDK
page-size default, `healthyOnly`, or Spotter dropping events. It is caused by a
server-side Nacos query cache that is not invalidated by persistent instance
registration events.

The authoritative path is:

```text
Persistent gRPC register
  -> PersistentClientOperationServiceImpl.onApply
  -> Client.addServiceInstance
  -> ClientRegisterServiceEvent / ServiceChangedEvent
  -> ServiceStorage cache is left intact
  -> ServiceQueryRequestHandler reads stale ServiceInfo
```

The Subscribe path differs:

```text
ServiceChangedEvent
  -> NamingSubscriberServiceV2Impl
  -> PushExecuteTask.generatePushData
  -> ServiceStorage.getPushData
  -> getAllInstancesFromIndex rebuilds the current list
  -> Subscribe receives the complete discovery view
```

This exactly matches the real evidence: Provider 1000/1000, Nacos write
acknowledgement 1000/1000, Subscribe 1000/1000, while gRPC Query is partial.

## Source proof

### Query reads the cached object

`naming/remote/rpc/handler/ServiceQueryRequestHandler.java` constructs the
service from namespace/group/name and calls `serviceStorage.getData(service)`.
It does not request a page and it passes the requested cluster and
`healthyOnly` only to the final selector.

### `ServiceStorage.getData` is cache-first

`naming/core/v2/index/ServiceStorage.java` contains:

```java
public ServiceInfo getData(Service service) {
    ServiceInfo data = serviceDataIndexes.get(service);
    return data != null ? data : getPushData(service);
}
```

`getPushData` rebuilds the host list from
`ClientServiceIndexesManager.getAllClientsRegisteredService(service)` and
stores it in `serviceDataIndexes`. `getData` does not refresh an existing
entry.

### Persistent registration does not invalidate `serviceDataIndexes`

`naming/core/v2/service/impl/PersistentClientOperationServiceImpl.java` applies
the Raft write, updates the persistent client, and publishes
`ClientRegisterServiceEvent` plus metadata events. It does not call
`ServiceStorage.removeData(service)` or otherwise refresh `serviceDataIndexes`.

The only `removeData` callers in the 3.2.4 source are service metadata deletion
and empty-service cleanup paths. There is no persistent instance add/update
invalidation path.

### Subscribe refreshes the cache before pushing

`naming/push/v2/task/PushExecuteTask.java` calls
`delayTaskEngine.getServiceStorage().getPushData(service)` before sending a
Subscribe update. That method always rebuilds the list and replaces the cached
entry. This is why Subscribe can see all instances even while Query sees an
older partial snapshot.

## Query parameter audit

The official Go SDK builds `ServiceQueryRequest` with only:

```text
namespace, serviceName, groupName, cluster, healthyOnly, udpPort
```

There is no `pageNo`, `pageSize`, `offset`, or `limit` in the gRPC request. The
Spotter call uses `cluster=<source>`, `healthyOnly=false`, and `udpPort=0`.
The query result variation across runs is therefore not a fixed pagination
boundary.

## Official issue search

No official issue matching this exact Nacos 3.2.4 standalone reproduction was
found. Related Nacos issues demonstrate the same class of CP/gRPC/query-view
inconsistency:

- Nacos 2.0 gRPC registration acknowledged while Console/OpenAPI did not show
  the service under a JRaft leader failure: issue #5361.
- Nacos cluster nodes showing different registered-service counts after Raft
  changes: issues #8099 and #8492.
- Nacos 3.0.2 gRPC connection failures causing instance visibility problems:
  issue #13955.

These are related evidence, not proof of the exact 3.2.4 defect. The source
inspection above is the direct proof for this reproduction.

## Impact on Spotter

This is an external Nacos query-plane defect. Switching Spotter from gRPC to
HTTP Query does not repair it; the local Nacos 3 Client OpenAPI diagnostic
returned `HTTP 200`, `code=0`, `data=[]` for the same persistent service.

The official SDK Subscribe path is the correct service-discovery observation
surface and passed the 5-second 1000-instance gate. The Query plane remains a
separate Nacos compatibility gate. Spotter must not treat an immediately stale
Query response as proof that a successful write was lost.

## Required upstream remediation

The Nacos server should invalidate or rebuild `ServiceStorage.serviceDataIndexes`
when persistent instance ADD/CHANGE/DELETE operations apply, before serving a
subsequent `ServiceQueryRequest`. A regression test should:

1. register multiple persistent instances through the gRPC persistent path;
2. issue Query immediately after the write acknowledgements;
3. assert Query and Subscribe return the same complete instance set;
4. repeat the test under a 1000-instance write wave.

Until that upstream behavior is fixed or a deployment upgrade proves it, the
Catalog/Query plane remains `NOT QUALIFIED` for the 5-second all-plane gate.
