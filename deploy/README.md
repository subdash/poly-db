# Deploying rust-kvs to Kubernetes

Manifests, scripts, and the verification checklist for running the single-node
store on a local [kind](https://kind.sigs.k8s.io/) cluster.

Design: [`docs/superpowers/specs/2026-09-10-rust-kvs-k8s-design.md`](../docs/superpowers/specs/2026-09-10-rust-kvs-k8s-design.md)

## Layout

```
deploy/cluster-up.sh        create the kind cluster, build and load the image, apply the manifests
deploy/reload.sh            rebuild the image, reload it into kind, restart the pod
deploy/k8s/namespace.yaml   the kvs namespace
deploy/k8s/service.yaml     kvs-headless (pod identity) and kvs (client traffic)
deploy/k8s/statefulset.yaml the StatefulSet, its PVC template, and the three probes
```

Every `kubectl` command below assumes `-n kvs`. To stop typing it:

```bash
kubectl config set-context --current --namespace=kvs
```

## Getting a cluster

```bash
./deploy/cluster-up.sh                              # first time
./deploy/reload.sh                                  # after changing Rust source
kubectl port-forward -n kvs svc/kvs 3000:3000 &     # to reach it from the host
```

The port-forward attaches to whichever pod backs the Service at the moment it
starts, and **dies when that pod dies**. Every experiment below that replaces the
pod requires re-establishing it. A `curl: (7) Failed to connect` after a restart
almost always means the forward is gone, not that the server is down.

## Verification

Five experiments. Four were run on 2026-09-11 against kind v1.34 on Docker
Desktop (Apple Silicon); the fifth is recorded but deferred, with its reason.
The observed output below is real, not illustrative.

### 1. Data survives pod deletion

The PersistentVolumeClaim and the replay path, working together.

```bash
curl -X PUT localhost:3000/v1/kv/alpha -H 'content-type: application/json' -d '{"value":"one"}'
curl -X PUT localhost:3000/v1/kv/beta  -H 'content-type: application/json' -d '{"value":"two"}'

kubectl delete pod kvs-0 -n kvs
kubectl wait --for=condition=ready pod/kvs-0 -n kvs --timeout=60s
# re-establish the port-forward here
curl -i localhost:3000/v1/kv/alpha
```

**Observed.** Both writes returned `204`. After deletion the pod came back in
roughly six seconds and both keys read back `200`:

```
{"key":"alpha","value":"one"}
{"key":"beta","value":"two"}
```

The telling pair is the two ages:

```
pod    kvs-0        1/1   Running   0     41s
pvc    data-kvs-0   Bound   pvc-ca646fa5-…-fe049eef15c2   1Gi   RWO   standard   11h
```

Same claim, same underlying volume UID, eleven hours old against a
forty-one-second pod. The pod was replaced; the storage was not. That is the
whole argument for `volumeClaimTemplates` in one line of output.

### 2. No acknowledged write is lost across a rolling update

Trigger a rollout by changing anything in the pod template:

```bash
kubectl patch statefulset kvs -n kvs -p \
  '{"spec":{"template":{"metadata":{"annotations":{"rollout":"2"}}}}}'
kubectl rollout status statefulset/kvs -n kvs
```

**Observed.** `alpha`, `beta`, and `gamma` all returned `200` with their values
afterwards. `kubectl rollout status` narrated the serialization plainly:

```
Waiting for partitioned roll out to finish: 0 out of 1 new pods have been updated...
Waiting for 1 pods to be ready...
partitioned roll out complete: 1 new pods have been updated...
```

**What this proves, and what it does not.** With one replica on a
`ReadWriteOnce` volume the update is necessarily serialized — the old pod must
release the volume before the new one can mount it. There is a window with no
server. The property demonstrated is that *no acknowledged write is lost*, not
that there is no downtime. Zero downtime requires replication.

**Two things that did not work as expected, both worth keeping:**

*The availability probe measured the wrong thing.* A `curl` loop against
`localhost:3000` during the rollout produced four `200`s and then fifty-six
connection failures that never recovered — not because the service stayed down,
but because the port-forward died with the old pod and nothing restarted it. A
host-side port-forward cannot measure in-cluster availability. Doing this
honestly means running the loop from a pod inside the cluster against
`kvs.kvs.svc.cluster.local`.

*The graceful shutdown is invisible.* `kubectl logs kvs-0 -f` across the
termination produced no output at all beyond the original startup line. The
shutdown sequence in `main` — SIGTERM, HTTP drain, channel close, `Engine::sync`,
thread join — runs silently, so there is no way to confirm from outside the
process that the final sync happened rather than the pod being killed mid-drain.
A `tracing::info!` at each stage of shutdown would make the termination grace
period observable. **This is a real gap and the most useful follow-up on this
list.**

### 3. Readiness gates traffic

Not a separate run — it falls out of experiment 1 if you poll the pod and the
Service's endpoint list together while the pod is being replaced.

```bash
kubectl delete pod kvs-0 -n kvs --wait=false
while true; do
  kubectl get pod kvs-0 -n kvs --no-headers
  kubectl get endpoints kvs -n kvs --no-headers
  sleep 2
done
```

**Observed.**

```
 2s  kvs-0 1/1 Terminating   endpoints: <none>
 4s  kvs-0 0/1 Running       endpoints: (empty)
 6s  kvs-0 1/1 Running       endpoints: 10.244.0.12:3000
```

Three distinct states in six seconds. At 2s the pod is still `1/1` — running and
passing its probes — but has already been removed from the endpoint list, because
deletion pulls a pod out of Service rotation before it stops it. At 4s a new pod
is `Running` and has an IP, and would happily accept a connection, but the
Service will not send it one: it is `0/1`, readiness has not passed, and the
endpoint list is empty. Only at 6s, once `/ready` answers, does the address
appear.

That gap between `Running` and *receiving traffic* is the entire point of a
readiness probe, and it is the mechanism replication will hang catch-up on.

`kubectl get endpoints` is also the first thing to check when a Service appears
broken. A selector that matches nothing is not an error — it produces a Service
with an empty endpoint list and connections that are simply refused.

### 4. The startupProbe earns its place

Remove the startupProbe, make liveness aggressive, and simulate a slow replay:

```bash
kubectl patch statefulset kvs -n kvs --type=strategic -p '{"spec":{"template":{"spec":{"containers":[{
  "name":"kvs",
  "command":["/bin/sh","-c","sleep 30; exec /usr/local/bin/kvs-server"],
  "startupProbe":null,
  "livenessProbe":{"httpGet":{"path":"/health","port":"http"},"periodSeconds":2,"failureThreshold":2,"timeoutSeconds":1}
}]}}}}'
```

The `sleep 30` stands in for a replay over a log large enough to outlast the
liveness threshold. The real log here is a few hundred bytes and replays
instantly, which is why this has to be simulated: without it the server starts
far too fast to ever fail a probe.

**Observed.** A clean restart loop that never makes progress:

```
 10s  kvs-0   0/1   Running   0
 35s  kvs-0   0/1   Running   1 (1s ago)
 75s  kvs-0   0/1   Running   2 (2s ago)
100s  kvs-0   0/1   Running   2 (27s ago)
```

Never `1/1`, never ready, restarting roughly every 35 seconds forever. `describe`
names the cause:

```
Warning  Unhealthy  Liveness probe failed: Get "http://10.244.0.14:3000/health": connect: connection refused
Normal   Killing    Container kvs failed liveness probe, will be restarted
```

This is exactly the failure mode the spec predicted for a log-structured store:
startup cost grows with the data, the pod is killed mid-replay, it restarts and
replays from the beginning, takes just as long, and is killed again. Every
restart makes zero progress. The startupProbe exists to suspend liveness and
readiness until the process has had a fair chance to come up.

**An accident worth reading.** The killed container's last state was:

```json
{"terminated":{"exitCode":137,"startedAt":"…T14:41:08Z","finishedAt":"…T14:41:42Z"}}
```

Exit 137 is SIGKILL, and the 34-second gap decomposes as roughly 4 seconds to
fail liveness twice plus the full 30-second `terminationGracePeriodSeconds`. The
container burned the entire grace period because `sh -c "sleep 30; …"` does not
forward SIGTERM to its child — the shell simply waits. That is precisely the
signal-handling bug the real image avoids by using an exec-form `ENTRYPOINT`, so
that `kvs-server` is PID 1 and receives SIGTERM itself. The experiment's scaffold
reproduced it by accident.

Restore with `kubectl apply -f deploy/k8s/statefulset.yaml`.

### 5. The keydir's memory ceiling — deferred

Lower `resources.limits.memory` to `64Mi` and write keys until the pod is
OOM-killed:

```bash
for i in $(seq 1 200000); do
  curl -s -o /dev/null -X PUT "localhost:3000/v1/kv/key-$i" \
    -H 'content-type: application/json' -d '{"value":"padding"}'
done
```

Expected: a restart, with `kubectl describe pod kvs-0 -n kvs` reporting
`Reason: OOMKilled` in the last state. Restore the limit to `512Mi` afterwards.

**Not run.** A sequential `curl` loop through a port-forward manages a few
hundred writes per second, so reaching a 64Mi keydir takes on the order of an
hour of wall clock and demonstrates the load generator more than the store. Doing
it properly means a concurrent writer running inside the cluster. The steps are
kept here because the property is real and worth confirming later.

The property itself is not in doubt, only the demonstration: the keydir holds
every key in memory, so the pod's memory limit is a hard ceiling on key count,
and crossing it presents as a crash rather than as a capacity error. Compaction
will not help — it reclaims disk, not index memory.

## Known gaps

- **Shutdown is unlogged.** See experiment 2. Nothing observable confirms
  `Engine::sync` ran before the process exited.
- **No in-cluster load generator.** Experiments 2 and 5 both want one; a
  host-side port-forward is the wrong instrument for both.
- **Channel capacity is untied to the memory limit.** The writer channel holds
  1024 slots and `MAX_VALUE_BYTES` is 1 MiB, so a saturated queue is ~1 GiB of
  pending writes against a 512Mi limit. Nothing currently relates the two.
