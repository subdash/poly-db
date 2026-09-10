# rust-kvs on Kubernetes: Design

**Date:** 2026-09-10
**Status:** Approved
**Scope:** Deploy the single-node MVP to a local Kubernetes cluster, and use the
cluster to demonstrate durability properties the store already claims.

## 1. Purpose and constraints

The MVP works: 66 tests green, a store that survives a crash, and a `main` that
shuts down gracefully. This project puts it in a container and runs it under an
orchestrator.

Two things shape the design.

**Kubernetes is the thing being learned.** The store is the excuse. Where a
Kubernetes object teaches something worth knowing — why a StatefulSet is not a
Deployment, what a startupProbe is for, how readiness differs from liveness — we
use it and we exercise it. Where a piece of production apparatus would add YAML
without teaching anything at this scale, we defer it and record why.

**The deployment must not be a dead end.** Replication is the destination. Every
object chosen here is the one that extends to N replicas without being unwound.
This is a stronger constraint than "get it running," and it is the reason for
several choices below that look oversized for a single pod.

### Out of scope

Replication and compaction — each gets its own spec. Also deferred, with
reasoning in section 7: PodDisruptionBudgets, metrics, structured JSON logging,
Kustomize overlays, distroless images, ConfigMaps, and multi-arch builds.

### Success criteria

The four experiments in section 6 run as described, and the pod comes back with
its data after being deleted.

## 2. Target environment

**kind** — Kubernetes in Docker — on macOS with Docker Desktop.

kind runs a cluster as containers on the local machine. It is free, disposable,
scriptable, and the standard choice for local development and CI. Images load
directly into the cluster with `kind load docker-image`, so no registry is
needed.

Neither kind nor a running Docker daemon is currently present on the development
machine, so installing both is the first task of the implementation plan.

The host is Apple Silicon, so images build natively for `linux/arm64` and no
cross-compilation is required.

## 3. Application changes

Small, and all in service of the container behaving like a well-mannered
Kubernetes citizen.

### 3.1 Bind address

`Config::addr` defaults to `127.0.0.1:3000`. Inside a container, the loopback
interface is private to that container: the kubelet's probes and all Service
traffic arrive on the pod's own address and are refused. The container must bind
`0.0.0.0:3000`.

The default stays as it is — binding a development server to all interfaces by
default is unfriendly — and the pod spec sets the address explicitly.

### 3.2 Configuration from the environment

Kubernetes supplies configuration through environment variables, not argv. clap
reads environment variables when its `env` feature is enabled:

```rust
#[arg(long, env = "KVS_ADDR", default_value = "127.0.0.1:3000")]
pub addr: SocketAddr,
```

Flags continue to work and take precedence over the environment; `--help`
documents both. Three attribute additions and one feature flag.

The variables are `KVS_ADDR`, `KVS_DATA_DIR`, and `KVS_FSYNC`.

### 3.3 A `/ready` endpoint

`GET /ready` returns `200` with `{"status":"ready"}`.

Today it is indistinguishable from `/health`: `Engine::open` replays the entire
log before `axum::serve` binds the listener, so a process that can answer at all
has already recovered. The value is not in what it currently reports but in the
separation it establishes:

- **Liveness** answers "should I be restarted?" A failure means the process is
  wedged and killing it is the remedy.
- **Readiness** answers "should I receive traffic?" A failure means the process
  is fine but not currently able to serve, and traffic should go elsewhere.

Wiring these to one endpoint conflates two questions with opposite consequences.
When replication lands, `/ready` reports catch-up state and the deployment needs
no change. Task 15 of the MVP plan recorded the same argument from the other
side, when it kept `/health` reporting OK while writes were failing: a liveness
check that fails under write pressure sheds capacity exactly when capacity is
scarce.

## 4. The container image

### 4.1 Structure

A two-stage build. The first stage uses `rust:1-bookworm` and produces a release
binary; the second copies that binary into `debian:bookworm-slim`. The result is
roughly 80 MB against about 1.5 GB for the builder.

**The Debian versions must match.** A binary linked against bookworm's glibc will
not run on bullseye, and the failure is `no such file or directory` naming a file
that visibly exists — the message refers to the missing dynamic loader, not to
the binary.

### 4.2 Base image choice

`debian:bookworm-slim`, chosen deliberately over the alternatives.

Distroless images are smaller and carry less attack surface, but they contain no
shell, so `kubectl exec -it -- sh` does not work. While learning, a shell in a
running pod is one of the more valuable things available. Alpine implies musl,
which needs a different Rust target and static-linking care that teaches nothing
about Kubernetes.

Distroless is recorded in section 7 as a hardening step for later.

### 4.3 Running as a non-root user

The image creates an unprivileged user; the pod spec sets `runAsNonRoot: true`.

A freshly provisioned PersistentVolume is owned by root, so a non-root process
cannot create its log file inside it. Setting `fsGroup` in the pod's security
context makes the kubelet change the volume's group ownership on mount. Without
it, startup fails with a permission error on the first thing the engine does.

### 4.4 Build context and caching

A `.dockerignore` excluding `target/` is required before the first build.
Otherwise Docker uploads the entire build directory as context on every
invocation.

A naive Dockerfile recompiles every dependency whenever any source file changes.
The standard remedy is to copy the manifests, build a stub, then copy the real
sources, so the dependency layer is invalidated only by manifest changes. This is
fiddlier in a workspace than in a single crate; `cargo-chef` automates it
correctly and is the fallback if the manual approach becomes tedious.

### 4.5 Getting the image into the cluster

`kind load docker-image kvs-server:dev` copies the image into the cluster's nodes
directly, with no registry involved.

**The image must carry a specific tag, not `latest`.** Kubernetes defaults
`imagePullPolicy` to `Always` when the tag is `latest` or absent, and to
`IfNotPresent` otherwise. An image tagged `kvs-server:latest` and loaded into
kind therefore fails with `ErrImagePull` — the cluster tries to pull from a
registry an image it already has — while `kvs-server:dev` works with no policy
set at all. The manifest states `imagePullPolicy: IfNotPresent` explicitly
anyway, so the behaviour does not depend on a tag-naming convention holding.

## 5. Kubernetes objects

Manifests live in `deploy/k8s/`, in a dedicated `kvs` namespace.

### 5.1 StatefulSet

`replicas: 1`, with a `volumeClaimTemplates` entry requesting 1Gi
`ReadWriteOnce` storage mounted at `/var/lib/kvs`.

A StatefulSet rather than a Deployment, for three reasons that all point at
replication:

1. **`volumeClaimTemplates` provisions storage per pod.** Scaling to three
   replicas creates three independent PersistentVolumeClaims, each with its own
   log directory. A Deployment shares one volume, which two engines appending to
   the same log would corrupt.
2. **Pod identity is stable.** `kvs-0` keeps its name and its volume across
   restarts and rescheduling.
3. **Updates are ordered.** Pods are replaced one at a time, waiting for each to
   become ready. With replication in place, this is the mechanism that keeps the
   service available during a rollout.

A Deployment with a hand-written PVC would additionally require
`strategy: Recreate`: a `ReadWriteOnce` volume attaches to one node at a time, so
the default rolling update deadlocks — the new pod cannot mount the volume until
the old pod releases it, and the old pod is not terminated until the new one is
ready.

Configuration is supplied as inline environment variables. A ConfigMap is the
right home once the values grow past a handful or a second object needs them; for
three variables it is indirection without benefit.

### 5.2 Services

Two, with different jobs.

**Headless** (`clusterIP: None`), named by the StatefulSet's `serviceName`. This
gives each pod individually addressable DNS —
`kvs-0.kvs.kvs.svc.cluster.local` — which is what a replica needs to reach a
specific peer. A normal Service load-balances and cannot address one pod.

**ClusterIP**, for client traffic. This is where readiness gating is observable:
a pod failing its readiness probe is removed from the Service's endpoint list and
stops receiving requests, without being restarted.

### 5.3 Probes

Three, and their interaction is the point.

| Probe | Endpoint | Failure means |
|---|---|---|
| `startupProbe` | `/health` | Still starting; suspends the other two |
| `livenessProbe` | `/health` | Restart the container |
| `readinessProbe` | `/ready` | Withhold traffic; do not restart |

The `startupProbe` gets a generous `failureThreshold` — about a 60-second window.
While it is failing, the liveness and readiness probes do not run at all.

Without it, a log large enough that replay exceeds the liveness threshold
produces an unrecoverable loop: the pod is killed mid-replay, restarts, replays
from the beginning, takes just as long, and is killed again. Every restart makes
no progress. This is a real failure mode for a log-structured store whose startup
cost grows with its data, and it is the reason the probe exists.

### 5.4 Resource requests and limits

Both are set explicitly, and the memory limit carries more meaning here than for
a stateless service.

The keydir is held entirely in memory, so the memory limit is a hard ceiling on
how many keys the store can hold. Exceeding it means the kernel OOM-kills the
pod, which presents as a crash rather than as a capacity problem. Naming the
limit makes that boundary visible rather than implicit.

### 5.5 Termination grace period

`terminationGracePeriodSeconds` is set explicitly.

On pod deletion the kubelet sends `SIGTERM`, waits this long, and then sends
`SIGKILL`. That window is where the shutdown sequence runs: graceful HTTP drain,
router dropped, channel closed, writer loop exits, `Engine::sync`, thread joined.
If the sequence overruns, the process is killed and the final sync does not
happen.

The default of 30 seconds is ample at MVP scale. It is stated in the manifest
because it connects the signal handling in `main` to the component that sends the
signal.

## 6. Verification

Access is by `kubectl port-forward svc/kvs 3000:3000`, so the `curl` commands
from the MVP work unchanged. Note that port-forward drops when the pod goes away,
which is itself informative during the restart experiments.

Four experiments, run and observed by hand:

**1. Data survives pod deletion.** Write keys, `kubectl delete pod kvs-0`, watch
the StatefulSet recreate it. The same PVC reattaches, the engine replays, the
keys are present. This is the PersistentVolumeClaim and the replay path working
together.

**2. No acknowledged write is lost across a rolling update.** Write keys, change
the pod template to trigger a rollout, confirm every key survives. This exercises
the full shutdown chain against a real orchestrator.

Note what is *not* claimed: with one replica and a `ReadWriteOnce` volume, a
rolling update is necessarily serialized — the old pod terminates before the new
one starts, so there is a window with no server. The property demonstrated is
that no acknowledged write is lost, not that there is no downtime. Zero downtime
requires replication.

**3. Readiness gates traffic.** `kubectl get pod -w` shows `0/1` before `1/1`, and
`kubectl get endpoints kvs` shows the Service's endpoint list empty and then
populated. A pod that is running still receives no traffic until it reports
ready.

**4. The startupProbe earns its place.** Remove it, set an aggressive liveness
threshold, and watch the pod restart-loop. Then restore it. Deliberately breaking
this is the fastest way to understand it, and it costs one `kubectl apply`.

Optionally, a fifth: set a small memory limit, write until the pod is OOMKilled,
and observe that the keydir-in-RAM design has a hard capacity ceiling that
presents as a crash.

**These stay manual.** The value is in watching the cluster react. Automating
them means writing rollout-waiting and retry logic that teaches nothing about
Kubernetes, and none of these are regressions likely to reappear silently. The
existing 66-test suite remains the automated safety net.

## 7. Future work

Deferred deliberately, with reasons.

**Operational apparatus.** A PodDisruptionBudget protects availability during
voluntary disruptions, which is meaningless with one replica. Prometheus metrics
and structured JSON logging are worth having once there is more than one pod to
correlate across. A Kustomize overlay layout separates dev from prod
configuration, which matters once a second environment exists. All of these are
better motivated after replication.

**Image hardening.** A distroless base once a shell is no longer needed for
exploration. Multi-architecture builds if this ever needs to run on an x86
cluster.

**Configuration.** A ConfigMap once the variable count grows; a Secret if
authentication is added.

**The other two projects.** Compaction next, then replication. This deployment
already provides what replication will need: `volumeClaimTemplates` for
per-replica storage, headless DNS for peer addressing, ordered rollout for
one-at-a-time updates, and a `/ready` endpoint that becomes the catch-up gate.
