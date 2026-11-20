# Spark Platform Learning Lab — Phase 1

## Status and safety boundary

This repository currently contains **deployment templates only**. No cluster, Helm release, container image, credential, or external GitHub repository has been created by this repository or by its authoring agent.

The target is a single-user, local learning lab. It is deliberately not a production platform and must not be exposed to the Internet. In particular, the JupyterHub configuration uses `DummyAuthenticator`, which is appropriate only for this local learning case.

The initial execution target is the Mac Studio (M5 Max, 64 GB RAM). The M5 MacBook Pro can be used for Local Mode and reduced-size tests, but its usable capacity must be measured before Kubernetes Client/Cluster experiments are accepted. The actual disk capacity, installed container runtime, and Minikube driver have not yet been checked.

## What these templates define

All resources live in the `spark-lab` namespace.

| File | Purpose | Safe to apply now? |
| --- | --- | --- |
| `k8s/00-namespace.yaml` | Isolates lab resources in one namespace. | Yes, after creating a cluster. |
| `k8s/10-spark-rbac.yaml` | Grants the Spark Driver only the namespaced access it needs to create and observe executors. | Yes, after creating a cluster. |
| `k8s/20-client-driver-service.yaml` | Stable in-cluster DNS name for an interactive Client-Mode Driver running in the Notebook Pod. | Yes, after JupyterHub is installed. |
| `k8s/30-silo.yaml` | Silo Deployment, Service, and PVC; it refers to a deliberately unresolved image and a manually created Secret. | No, fill the review fields first. |
| `k8s/silo-credentials.example.yaml` | Shape of the Silo credential Secret. It contains no credentials and must not be committed as a real secret. | No. |
| `jupyterhub/values.yaml` | Values file for the Zero-to-JupyterHub Helm chart. It refers to a deliberately unresolved custom notebook image. | No, fill the review fields first. |
| `k8s/templates/90-networkpolicy-optional.yaml` | A restrictive template to use only after the CNI and required network ranges are known. | No. |

No image reference is silently selected: `REPLACE_*` fields must be replaced with an immutable, reviewed image digest before use. This prevents a template from becoming an accidental unreviewed deployment.

## Architecture and mode definitions

### Local Mode

The Jupyter kernel process in the Notebook Pod creates `SparkContext` with `master=local[*]`. That process is the Driver and the work runs in local threads. Kubernetes creates no Executor Pods.

### Kubernetes Client Mode

The Jupyter kernel process in the Notebook Pod is the Driver. It authenticates to the Kubernetes API using the `spark-client` ServiceAccount and asks Kubernetes to create Executor Pods from the Spark runtime image. The headless Service `spark-client-driver` gives those Executors a stable DNS address for the Driver:

```text
executor pod
  -> spark-client-driver.spark-lab.svc:7078  (Driver RPC)
  -> spark-client-driver.spark-lab.svc:7079  (BlockManager)
  -> Notebook Pod / Jupyter kernel (the Driver process)
```

The fixed `spark-lab/driver=interactive-client` label is intentionally valid only for one active personal Notebook Driver. This is not a multi-user design. At session startup, the notebook must discover its own Pod name via the Downward API and set `spark.kubernetes.driver.pod.name` to that exact name. Spark then adds owner references to Executor Pods, allowing Kubernetes to delete them if that Driver Pod is removed.

`spark.driver.host`, `spark.driver.port`, and `spark.blockManager.port` must be set **before** `SparkContext` is created. The recommended fixed ports are 7078 (Driver RPC), 7079 (BlockManager), and 4040 (Spark UI, incremented by Spark if occupied). The Spark UI remains accessed through a deliberate `kubectl port-forward` during Phase 1 rather than being publicly exposed.

### Kubernetes Cluster Mode

The Notebook only edits and runs `spark-submit`. Kubernetes starts a separate Driver Pod from the selected Spark runtime image; that Driver then creates Executor Pods. The Notebook Pod is neither the Driver nor an Executor. This is why Cluster Mode makes Driver resource requests, logs, restart/cleanup behavior, and Python environment independently observable.

Apache Spark supports both Kubernetes Client and Cluster deployment modes. Client Mode is useful here specifically to reveal the interactive-notebook networking and lifecycle problem; Cluster Mode is the more natural fit for declarative batch workloads and Spark operators. Spark on Kubernetes is a mainstream supported deployment backend, not a requirement for every user or startup. Managed offerings such as Databricks, EMR, Dataproc, or serverless Spark reduce operations work; Kubernetes is most justified where a team already operates Kubernetes and needs shared scheduling, image control, or platform integration.

## Why the network objects exist

| Item | What it does | Why it is required here |
| --- | --- | --- |
| Stable label | Marks the one Notebook Pod that may host the interactive Driver. | The Service selector needs a predictable target. |
| Headless Service | Publishes the selected Pod IP under stable cluster DNS without load balancing. | Executors need a routable Driver hostname rather than an ephemeral Pod IP. |
| Driver port | Spark RPC endpoint (7078 in this lab). | Executors register and receive work from the Driver. |
| BlockManager port | Spark block transfer endpoint (7079 in this lab). | Executors and Driver exchange cached/shuffle block data. |
| `spark-client` ServiceAccount + Role | Namespaced permission to create, watch, and delete Spark Executor resources. | In Client Mode the Driver is inside the Notebook Pod but still controls Executor Pods. |
| Optional NetworkPolicy | Restricts allowed traffic after baseline connectivity is proven. | Enabling it before knowing the CNI, DNS, API and Service CIDRs can break the lab. |

The Role is namespace-scoped and deliberately does not use a ClusterRole. It permits the Driver to manage only the Pods, Services, ConfigMaps, and Events needed in `spark-lab`. It does not grant access to Secrets.

## Python dependencies: Notebook versus Executors

Installing a Python package in the Notebook Pod changes only the Notebook environment. It does **not** put that package in an Executor Pod and should not be treated as a dependency-distribution method.

Phase 1 will compare these methods, in this order:

1. **Pinned custom Spark runtime image — baseline.** Build one reviewed image containing Spark, Python, Java, the S3A/JVM dependencies, and Python packages. Cluster Mode uses it for Driver and Executors; Client Mode uses it for Executors and a compatible Notebook image for the Driver.
2. **`--py-files` / `spark.submit.pyFiles` — small pure-Python project code.** This distributes a zip/wheel-like Python artifact to Executors, but does not solve native libraries, JVM JARs, or arbitrary OS packages.
3. **Packed Python environment — only if required.** This needs a separate compatibility decision for the selected Spark/Python version.

Manual `pip install` in a CML-like terminal is useful for exploration but is not a reproducible way to provision distributed Executors. Commercial systems can expose a terminal in a session container, yet their exact Pod topology is product/version/runtime-specific. CML documentation confirms containerized Spark execution and separately managed Spark Driver/Executor Pods; it does not justify assuming that every CML Notebook always receives a distinct Driver Pod. This lab therefore measures its own topology instead of claiming it matches CML.

## Silo decision

Silo remains the selected first candidate. The deployment template uses its S3-compatible API endpoint internally as `http://silo.spark-lab.svc:9000`. Before any apply, pin a reviewed Silo release digest, verify its AGPL-3.0 license is acceptable for this personal lab, create an experiment-only access key, and test bucket/object operations. Spark must then prove reads and writes from Executors, not merely from the Driver.

## Manual deployment sequence — do not run until approved

The following commands are documentation only. They will download software or images when you run them yourself; none has been executed by the authoring agent. Confirm Minikube driver, CPU architecture, disk, exact versions, and image digests first.

```text
1. Install/verify Docker-compatible runtime, kubectl, Minikube, and Helm for Apple Silicon.
2. Start a Minikube profile with explicitly chosen CPU, memory, and disk values.
3. Replace every REPLACE_* image reference with a pinned arm64/multi-arch digest.
4. Create a real Silo Secret locally (never commit it), then apply namespace, RBAC, and Silo resources.
5. Add the official JupyterHub Helm repository and install the chart using jupyterhub/values.yaml.
6. Build and load/publish the reviewed Notebook and Spark runtime images.
7. Spawn the single Notebook user; then apply the headless Driver Service.
8. Run Local WordCount, Client WordCount, Cluster WordCount, then S3A WordCount in that order.
9. Record Pod descriptions, Events, logs, Spark UI evidence, image digests, and final configuration in this README.
```

### Copyable manual command templates

Choose the container runtime first. The `docker` driver below is valid only if
that runtime exposes a working Docker-compatible CLI; replace it with the
verified driver otherwise. Values in angle brackets are mandatory decisions,
not defaults.

```bash
# Preconditions: these commands are intentionally for the operator to run,
# not commands that this repository has run.
minikube start --profile spark-lab --driver=<verified-driver> \
  --cpus=<approved-cpu-count> --memory=<approved-memory-mb> \
  --disk-size=<approved-disk-size>

kubectl config use-context spark-lab
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/10-spark-rbac.yaml

# Pull only reviewed immutable arm64 or multi-architecture images.
docker pull <notebook-image@sha256:reviewed-digest>
docker pull <spark-runtime-image@sha256:reviewed-digest>
docker pull <pgsty/silo@sha256:reviewed-digest>

# Load locally pulled images only when the selected Minikube driver requires it.
minikube image load --profile spark-lab <notebook-image@sha256:reviewed-digest>
minikube image load --profile spark-lab <spark-runtime-image@sha256:reviewed-digest>
minikube image load --profile spark-lab <pgsty/silo@sha256:reviewed-digest>

# Create the real secret without placing credentials in a YAML file or Git.
kubectl --namespace spark-lab create secret generic silo-root-credentials \
  --from-literal=accessKey='<experiment-only-access-key>' \
  --from-literal=secretKey='<experiment-only-secret-key>'

# After all REPLACE_* values are filled and reviewed:
kubectl apply -f k8s/30-silo.yaml
helm repo add jupyterhub https://hub.jupyter.org/helm-chart/
helm repo update
helm upgrade --install jupyterhub jupyterhub/jupyterhub \
  --namespace spark-lab --create-namespace \
  --version <reviewed-chart-version> \
  --values jupyterhub/values.yaml

# Apply only after JupyterHub has spawned the single Notebook Pod.
kubectl apply -f k8s/20-client-driver-service.yaml
```

The `docker pull`, `minikube image load`, Helm repository update, and Helm
installation commands may download artifacts. Run them manually only after
the exact versions/digests are recorded and approved. Do not run the optional
NetworkPolicy template until its two CIDR placeholders are replaced with
values observed from the actual cluster.

For the planned Mac Studio baseline, reserve resource headroom instead of allocating all 64 GB to Minikube. The exact allocation is intentionally not specified until the host disk, container runtime, and desired executor count are measured.

## Required pre-deployment decisions

- Exact Spark, Python, Java, Hadoop-AWS, AWS SDK, JupyterHub chart, Minikube, and Silo versions.
- Reviewed arm64 or multi-architecture image digests for Notebook, Spark, and Silo.
- StorageClass and PVC capacity/retention policy.
- GitHub repository and local-only credential injection method.
- Silo endpoint/TLS choice and Spark S3A connector compatibility matrix.
- Host resource measurement and target initial executor count.

## Sources checked during design

- Apache Spark: [Running Spark on Kubernetes](https://spark.apache.org/docs/3.5.7/running-on-kubernetes.html)
- Apache Spark: [Spark 3.5.7 overview and runtime support](https://spark.apache.org/docs/3.5.7/)
- Zero to JupyterHub: [Authentication](https://z2jh.jupyter.org/en/4.4.3/administrator/authentication.html)
- Zero to JupyterHub: [configuration reference](https://z2jh.jupyter.org/en/latest/reference/reference.html)
- PGSTY: [Silo repository](https://github.com/pgsty/silo)
- Cloudera AI: [Spark on Kubernetes](https://docs.cloudera.com/machine-learning/cloud/architecture-overview/topics/ml-architecture-overview-spark-on-kubernetes.html)
