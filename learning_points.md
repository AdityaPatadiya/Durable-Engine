## Question-1: What is staging and production? and is it similar to development and production?

**Development → Staging → Production**

| Environment | Purpose | Who uses it |
|-------------|---------|-------------|
| **Development** | Where you write and test code locally. Broken code is fine. Uses mock services, local Docker Compose, etc. | You (the developer) |
| **Staging** | A near-exact replica of production used to validate before going live. Real services, real configs, but with test data. | QA team, devs doing final validation |
| **Production** | The live system serving real users/data. Must be stable, monitored, and hardened. | End users / customers |

---

## How they differ in this project

| Setting | Development | Staging | Production |
|---------|-------------|---------|------------|
| **Replicas** | 1 (local) | 2 | 3 |
| **Autoscaling** | None | 2–5 pods | 3–10 pods |
| **Resources** | Whatever your machine has | 250m–2 CPU, 512Mi | 1–4 CPU, 1Gi |
| **Endpoints** | `localhost` / mock sinks | `api-staging.example.com` | `api.example.com` |
| **Log level** | `DEBUG`/`INFO` | `INFO` | `WARNING` |
| **Secrets** | Hardcoded / none | Inline in K8s Secret | External (Vault, Sealed Secrets) |
| **PDB** | No | Yes (min 1 pod) | Yes (min 50%) |
| **How you run it** | `make run` / `docker-compose up` | `make helm-install-staging` | `make helm-install-production` |

---

## Why staging exists (and dev doesn't get a K8s overlay)

- **Development** = your local machine. You already have `docker-compose.yml` and `make run` for that. No need for Kubernetes.
- **Staging** = deployed on a real K8s cluster, but with lower resources and test data. It's where you catch issues that don't show up locally (networking, scaling, resource limits, real service dependencies).
- **Production** = the real deal — hardened, autoscaled, anti-affinity for fault tolerance.



# Question-2: there are different kind, what are these kind and how many total kind values are exists for the kuberetes? and why we didn't use the pod kind for the staging?

## Kubernetes `kind` Values

There are 40+ built-in kinds in Kubernetes. Here are the most commonly used ones, grouped by category:

---

### Workloads (run your code)

| Kind | Purpose |
|------|---------|
| **Pod** | Smallest deployable unit — a single container (or group of containers) |
| **Deployment** | Manages Pods with rolling updates, replicas, rollbacks |
| **StatefulSet** | Like Deployment but for stateful apps (stable network identity, ordered startup) |
| **DaemonSet** | Runs one Pod on every node (for logging agents, monitoring, etc.) |
| **Job** | Runs a Pod to completion (one-time task) |
| **CronJob** | Runs a Job on a schedule |
| **ReplicaSet** | Ensures N pod replicas exist (Deployment creates this for you) |

---

### Networking (expose your code)

| Kind | Purpose |
|------|---------|
| **Service** | Stable IP/DNS to reach your Pods (`ClusterIP`, `NodePort`, `LoadBalancer`) |
| **Ingress** | HTTP/HTTPS routing from outside the cluster to Services |
| **NetworkPolicy** | Firewall rules between Pods |

---

### Configuration

| Kind | Purpose |
|------|---------|
| **ConfigMap** | Non-sensitive config (our `engine.yaml` lives here) |
| **Secret** | Sensitive data (passwords, tokens) |

---

### Storage

| Kind | Purpose |
|------|---------|
| **PersistentVolumeClaim** | Request for storage (our DLQ and output dirs) |
| **PersistentVolume** | Actual storage provisioned by admin/cloud |
| **StorageClass** | Defines storage types (SSD, HDD, etc.) |

---

### Scaling & Availability

| Kind | Purpose |
|------|---------|
| **HorizontalPodAutoscaler** | Auto-scale Pods based on CPU/memory |
| **PodDisruptionBudget** | Minimum available Pods during maintenance |

---

### Access Control

| Kind | Purpose |
|------|---------|
| **ServiceAccount** | Identity for Pods to call K8s API |
| **Role / ClusterRole** | Permission definitions |
| **RoleBinding / ClusterRoleBinding** | Assigns roles to users/service accounts |

---

### Cluster-Level

| Kind | Purpose |
|------|---------|
| **Namespace** | Virtual cluster isolation |
| **Node** | A worker machine in the cluster |
| **ResourceQuota** | Limits resource usage per namespace |
| **LimitRange** | Default resource limits for Pods |

---

### Custom (from operators)

| Kind | Purpose |
|------|---------|
| **ServiceMonitor** | Prometheus Operator — auto-discover metrics endpoints |
| **Certificate** | cert-manager — auto-provision TLS certs |
| **Any CustomResourceDefinition (CRD)** | Extends Kubernetes with custom kinds |

---

## Why we use `Deployment` instead of `Pod`

You never deploy a raw `Pod` in staging/production. Here's why:
```
Pod (raw)                          Deployment
────────────                       ──────────
- Single instance                  - Manages N replicas
- If it dies, it stays dead        - If a Pod dies, it auto-restarts a new one
- No rolling updates               - Zero-downtime rolling updates
- No rollback                      - One command rollback
- No scaling                       - HPA can auto-scale it
- No health-based restart          - Restarts unhealthy Pods automatically
```

---

## What actually happens
```
You create:    Deployment (replicas: 2)
                   │
K8s creates:   ReplicaSet
                   │
K8s creates:   Pod-1    Pod-2
```

A `Deployment` creates a `ReplicaSet`, which creates the Pods. You never manage Pods directly.

---

## When would you use a raw `Pod`?

Almost never. Only for:

- Quick debugging (`kubectl run debug --image=busybox`)
- One-off diagnostics
- Our Helm test pod (`test-connection.yaml` uses a raw Pod because it runs once and exits)


--
# Learning Points 
## Reduce the Dockerfile size:
- Remove the `node_modules` when you don't need the node to and the output is fully static
    - static meand any react, vue, angular etc... framework or library will generate the static files after the build so we need to identify that after `npm run build`, can I serve it with just nginx and if `yes` then I can remove the `node_modules`.
    - and frameworks or libraies like Next.js, Nest.js, Express / Fastify, etc... are provide dynamic output so it requires node and node.js server running so we need to keep the `node_modules`.

Large size 1.37+ GB
```Dockerfile
FROM node: 22

WORKDIR /app

COPY package*.json./

COPY

RUN npm install

EXPOSE 3000

CMD ["node", "index.js"]
```
Same application with reduced size under 200 MB:
```Dockerfile
FROM node: 22-alpine AS builder

WORKDIR /app

COPY package*.json ./

RUN npm ci --only=production \ && npm cache clean--force

COPY

FROM node: 22-alpine

WORKDIR /app

COPY --from=builder /app/app

EXPOSE 3000

CMD ["node", "index.js"]
```

- The reason of using the multi-stage builder is that when in the first stage if we do `npm ci` then it will also include the hiddne/cached files in the layer and it's size will be around 80 MB but when in the 2nd stage if we do it will just copy the files from the first stage now the caches and hidden files
