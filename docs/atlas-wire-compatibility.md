# Atlas wire compatibility gate

Status: **NOT VERIFIED (P1)**.

The repository still ships a self-contained mirror of the private
`beehive-proto api/service/v2` surface.  `pkg/beehive/service/v2/v2.go` defines
ordinary Go structs and aliases them to the domain model; it does not contain
generated protobuf descriptors or `ProtoReflect` methods.  Consequently the
default gRPC protobuf codec cannot marshal these values.  The zero-option
`pkg/discoverycenter.Dial` path explicitly forces the repository's JSON codec
and invokes the following method paths:

| RPC | Method path | Current request/response wire | Evidence |
| --- | --- | --- | --- |
| Incremental sync | `/service.v2.InstanceService/SynInstance` | JSON encoding of mirror structs | discoverymock bufconn/TCP tests |
| Full sync | `/service.v2.InstanceService/SynAllInstance` | JSON encoding of mirror structs | discoverymock bufconn/TCP tests |
| Read all | `/service.v2.InstanceService/GetAllInstance` | JSON encoding of mirror structs | discoverymock bufconn/TCP tests |

The discoverymock and all in-process E2E tests use the same JSON codec.  They
prove request routing, field round-trip and error handling only; they do **not**
prove that a production Atlas accepts JSON, uses these method paths, exposes
the same field names, or accepts the payload size and TLS/authentication
configuration used by deployment.

## Real-target gate

An opt-in test is provided at `tests/e2e/atlas_real_test.go` with build tag
`atlas_real`.  It executes `Dial → SynInstance → SynAllInstance →
GetAllInstance` against a disposable target and checks that the canary appears
in the returned list.  The test sends writes only when both explicit guards
are present:

```text
ATLAS_REAL_ADDR=<scratch host:port>
ATLAS_REAL_ALLOW_WRITE=1
ATLAS_REAL_SCRATCH=1
```

Optional variables are `ATLAS_REAL_APP_CODE`, `ATLAS_REAL_PROVIDER`,
`ATLAS_REAL_STATUS`, `ATLAS_REAL_INSTANCE_SUFFIX` and `ATLAS_REAL_TIMEOUT`.
The command is:

```bash
go test -tags=atlas_real ./tests/e2e/... -run TestAtlasReal -count=1
```

Missing endpoint or write guards produce an explicit `NOT VERIFIED` skip; a
JSON rejection, non-zero response, missing canary, or timeout is a test
failure.  No endpoint, Atlas version, proto source, TLS certificate or auth
contract is invented in this repository.  A passing run must record the Atlas
version/digest, endpoint, codec, method status, latency, payload size and
cleanup result in an external evidence artifact before this document can be
promoted to PASS.

The sentinel test `pkg/beehive/service/v2/wire_compat_test.go` intentionally
asserts that the current mirror types are not generated protobuf messages.  If
that test starts failing, the implementation has changed and the real-wire
gate must be rerun (or a versioned generated-proto adapter must be introduced)
before release.

## Required follow-up for PASS

1. Obtain the target Atlas service version, authoritative proto files, TLS and
   authentication requirements, and the RPC contract from the service owner.
2. Run the tagged test against a disposable/pre-production Atlas and preserve
   the complete evidence tuple listed above.
3. If Atlas requires protobuf, generate the client from those exact proto
   files, keep the JSON mirror behind an explicit compatibility feature flag,
   and add protobuf-vs-JSON contract tests before changing the default.
4. Keep discoverymock JSON tests labelled `mock-pass`; never use them as
   production protocol evidence.
