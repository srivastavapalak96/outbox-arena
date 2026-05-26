# K8s manifests

Kustomize manifests for outbox-arena. The `base/` directory has the full set of
resources (namespace, Postgres StatefulSet, Strimzi Kafka cluster, six service
Deployments with their Services, plus an HPA on order-service). Overlays
under `overlays/` apply environment-specific patches.

## Quickstart on kind

```bash
# 1. Cluster up
kind create cluster --name outbox-arena --image kindest/node:v1.30.0
kubectl cluster-info --context kind-outbox-arena

# 2. Strimzi operator (one-time per cluster)
kubectl create namespace strimzi-system
kubectl apply -f 'https://strimzi.io/install/latest?namespace=strimzi-system' -n strimzi-system

# 3. Build local images directly into the kind cluster
# Requires Jib's gradle plugin; loads each image via kind load docker-image after the
# jibDockerBuild step.
for svc in order-service payment-service inventory-service shipping-service projection-service notifier-sink; do
  ./gradlew :modules:${svc}:jibDockerBuild -Pimage.tag=local
  kind load docker-image --name outbox-arena ghcr.io/srivastavapalak96/outbox-arena/${svc}:local
done

# 4. Apply
kubectl apply -k k8s/overlays/local-kind

# 5. Watch the pods come up (give Strimzi 60-90s to provision the Kafka cluster)
kubectl -n outbox-arena get pods -w

# 6. Reach the order-service via NodePort
curl localhost:30081/actuator/health
```

## What's in here

| Path | What |
|------|------|
| `base/namespace.yaml` | `outbox-arena` namespace |
| `base/postgres/{configmap,statefulset,service}.yaml` | Single-instance Postgres 16 with `wal_level=logical` and the same per-service-role bootstrap as `infra/postgres/init.sql` |
| `base/kafka/cluster.yaml` | Strimzi `Kafka` + `KafkaNodePool` for a single-broker KRaft cluster |
| `base/services/*.yaml` | One Deployment + Service per saga participant + projection-service + notifier-sink |
| `base/services/order-service.yaml` | Includes an HPA scaling 2-10 replicas on CPU; the operationally-meaningful `outbox_unpublished_count` HPA rule is documented (commented out -- needs the Prometheus adapter custom-metrics API installed) |
| `overlays/local-kind/kustomization.yaml` | NodePort patches for `order-service` and `projection-service`, image-tag overrides for locally built images |

## What's NOT in here yet

- Debezium connector registration via a Strimzi `KafkaConnector` CR. Manual via
  `make register-connector` after the cluster is up; CR-based version planned.
- TLS / Service Mesh. Strimzi can configure mutual TLS in 5 lines; left out for
  the local-kind overlay to keep the dev loop fast.
- Network policies. Production overlay should add them.
- Prometheus adapter for custom-metric HPAs.
- `dev` and `prod` overlays. Left as `PLANNED` markers in the parent plan.

## Validation

`kubectl kustomize k8s/overlays/local-kind` renders ~660 lines of YAML with no
warnings. The actual `kubectl apply` against a kind cluster is on the week-10
final checklist.
