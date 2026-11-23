# Spark2026 multi-user: per-user namespace provisioning and cleanup.
#
# Loaded into the Hub via `hub.extraConfig` (startcluster.sh passes this file
# with `helm --set-file`). It runs inside the Hub Pod, which has the
# cluster-scoped permissions defined in k8s/11-hub-rbac.yaml.
#
# Design: docs/spark_usermanagement_design.md (H1 + Pod Security baseline).
#
# What it does:
#   * every user gets namespace  user-<name>  (KubeSpawner.enable_user_namespaces)
#   * a pre_spawn_hook creates, in that namespace, everything the chart's
#     KubeSpawner does NOT create: the spark-runner ServiceAccount + RoleBinding,
#     a copy of the shared Silo credentials, the two Spark UI Services, a
#     ResourceQuota and a LimitRange. The namespace itself carries the Pod
#     Security "baseline" labels so a user cannot launch a privileged / hostPath
#     Pod (the escalation path analysed in the design doc).
#   * delete_forever() removes the whole namespace, the user's Silo objects
#     (s3a://spark-data/users/<name>/) and the user's Spark event logs.
import json
import re

from kubernetes_asyncio import client, config as k8s_config
from tornado.httpclient import AsyncHTTPClient
from kubespawner import KubeSpawner

PLATFORM_NS = "spark-lab"
POLICY_VERSION = "v1.36"            # pin PSA policy version (see design doc §5 #3)
SILO_SHARED_SECRET = "silo-shared-credentials"
SPARK_RUNNER_SA = "spark-runner"
SPARK_RUNNER_CLUSTERROLE = "spark-runner"
# Same image digest as k8s/30-silo.yaml; it bundles the mcli client.
SILO_IMAGE = "pgsty/silo@sha256:c20d112390b9a9ce8c11b8af95ad35bcaa62f4759663c09c936cd8d2b511f329"
HISTORY_API = "http://spark-history.spark-lab.svc.cluster.local:18080/api/v1/applications?limit=100000"
EVENT_LOG_PREFIX = "local/spark-data/spark-logs"
# Spark application IDs look like local-1795445427420 or spark-<32 hex>.
_APP_ID = re.compile(r"^[A-Za-z0-9_-]+$")

# Labels put on every user namespace. Pod Security "baseline" is enforced;
# "restricted" is only warned/audited (the Notebook and Spark pods run as root
# and would fail restricted). spark-lab/tenant marks it for bulk cleanup.
def _namespace_labels(username: str) -> dict:
    return {
        "spark-lab/tenant": "true",
        "spark-lab/user": username,
        "pod-security.kubernetes.io/enforce": "baseline",
        "pod-security.kubernetes.io/enforce-version": POLICY_VERSION,
        "pod-security.kubernetes.io/warn": "restricted",
        "pod-security.kubernetes.io/warn-version": POLICY_VERSION,
        "pod-security.kubernetes.io/audit": "restricted",
        "pod-security.kubernetes.io/audit-version": POLICY_VERSION,
    }


async def _ignore_conflict(coro):
    """Await a create call; treat 409 AlreadyExists as success."""
    try:
        await coro
    except client.ApiException as exc:
        if exc.status != 409:
            raise


async def _provision_namespace(username: str, log):
    ns = f"user-{username}"
    k8s_config.load_incluster_config()  # sync in kubernetes_asyncio
    async with client.ApiClient() as api_client:
        core = client.CoreV1Api(api_client)
        rbac = client.RbacAuthorizationV1Api(api_client)

        # Namespace with the Pod Security + tenant labels. If it already exists
        # (KubeSpawner may have created it on a previous spawn), patch the labels
        # so a change to this file takes effect.
        labels = _namespace_labels(username)
        meta = client.V1ObjectMeta(name=ns, labels=labels)
        await _ignore_conflict(
            core.create_namespace(client.V1Namespace(metadata=meta))
        )
        await core.patch_namespace(ns, {"metadata": {"labels": labels}})

        # spark-runner ServiceAccount: used by the Notebook (Client Mode driver)
        # and by Cluster Mode drivers.
        await _ignore_conflict(
            core.create_namespaced_service_account(
                ns, client.V1ServiceAccount(
                    metadata=client.V1ObjectMeta(name=SPARK_RUNNER_SA))
            )
        )

        # Bind the cluster-wide spark-runner ClusterRole into THIS namespace
        # only. A RoleBinding to a ClusterRole grants the ClusterRole's verbs
        # scoped to the binding's namespace.
        rb = client.V1RoleBinding(
            metadata=client.V1ObjectMeta(name=SPARK_RUNNER_SA, namespace=ns),
            role_ref=client.V1RoleRef(
                api_group="rbac.authorization.k8s.io",
                kind="ClusterRole",
                name=SPARK_RUNNER_CLUSTERROLE,
            ),
            subjects=[client.RbacV1Subject(
                kind="ServiceAccount", name=SPARK_RUNNER_SA, namespace=ns)],
        )
        await _ignore_conflict(rbac.create_namespaced_role_binding(ns, rb))

        # Copy the shared (non-root) Silo credentials from the platform namespace.
        src = await core.read_namespaced_secret(SILO_SHARED_SECRET, PLATFORM_NS)
        copied = client.V1Secret(
            metadata=client.V1ObjectMeta(name=SILO_SHARED_SECRET, namespace=ns),
            data=src.data, type=src.type,
        )
        await _ignore_conflict(core.create_namespaced_secret(ns, copied))
        await core.patch_namespaced_secret(
            SILO_SHARED_SECRET, ns, {"data": src.data})

        # Spark UI Services (reached through the Portal at /spark-ui/<user>/ and
        # /spark-driver/<user>/). publishNotReadyAddresses so the UI is reachable
        # the moment the pod has an IP.
        ui_svc = client.V1Service(
            metadata=client.V1ObjectMeta(name="spark-ui", namespace=ns),
            spec=client.V1ServiceSpec(
                publish_not_ready_addresses=True,
                selector={"component": "singleuser-server"},
                ports=[client.V1ServicePort(name="spark-ui", port=4040, target_port=4040)],
            ),
        )
        await _ignore_conflict(core.create_namespaced_service(ns, ui_svc))
        drv_svc = client.V1Service(
            metadata=client.V1ObjectMeta(name="spark-driver-ui", namespace=ns),
            spec=client.V1ServiceSpec(
                publish_not_ready_addresses=True,
                selector={"spark-role": "driver"},
                ports=[client.V1ServicePort(name="spark-ui", port=4040, target_port=4040)],
            ),
        )
        await _ignore_conflict(core.create_namespaced_service(ns, drv_svc))

        # ResourceQuota: cap only requests and object counts (not limits), so
        # Spark pods that set requests but no cpu limit are not rejected.
        # Sizing (requests): Notebook 1 CPU / 2G; a Spark pod with 1g heap is
        # ~1.4Gi (heap + overhead). Cluster Mode = Notebook + Driver + 2
        # Executors ~= 6.1Gi, so memory is capped at 8Gi.
        quota_hard = {
            "requests.cpu": "4",
            "requests.memory": "8Gi",
            "pods": "12",
            "persistentvolumeclaims": "2",
        }
        quota = client.V1ResourceQuota(
            metadata=client.V1ObjectMeta(name="user-quota", namespace=ns),
            spec=client.V1ResourceQuotaSpec(hard=quota_hard),
        )
        await _ignore_conflict(core.create_namespaced_resource_quota(ns, quota))
        await core.patch_namespaced_resource_quota(
            "user-quota", ns, {"spec": {"hard": quota_hard}})

        # LimitRange: default request for any container that sets none (e.g. the
        # prepare-home init container), so the ResourceQuota above does not reject
        # the pod for a missing request.
        lr = client.V1LimitRange(
            metadata=client.V1ObjectMeta(name="defaults", namespace=ns),
            spec=client.V1LimitRangeSpec(limits=[client.V1LimitRangeItem(
                type="Container",
                default_request={"cpu": "100m", "memory": "256Mi"},
            )]),
        )
        await _ignore_conflict(core.create_namespaced_limit_range(ns, lr))

    log.info("spark-lab: provisioned namespace %s", ns)


async def _cleanup_user(username: str, log):
    ns = f"user-{username}"

    # Delete the whole namespace (Notebook PVC, pods, SA, RoleBinding, Secret,
    # Services, Quota all go with it).
    k8s_config.load_incluster_config()
    async with client.ApiClient() as api_client:
        core = client.CoreV1Api(api_client)
        try:
            await core.delete_namespace(ns)
            log.info("spark-lab: deleting namespace %s", ns)
        except client.ApiException as exc:
            if exc.status != 404:
                raise

        # Delete the user's Silo objects and event logs with a one-shot mcli pod
        # in the platform namespace (the Hub image has no S3 client). Best-effort.
        try:
            app_ids = await _user_app_ids(username, log)
            await _run_silo_cleanup(core, username, app_ids, log)
        except Exception as exc:  # noqa: BLE001 - cleanup must not block deletion
            log.warning("spark-lab: Silo cleanup for %s failed: %s", username, exc)


async def _user_app_ids(username: str, log) -> list:
    """Application IDs whose sparkUser is this user, from the History Server.

    Event log names contain only the application ID, so the History Server's
    listing is the only place that maps logs to users. sparkUser comes from
    SPARK_USER, which the platform sets but a user can override (accepted).
    """
    try:
        resp = await AsyncHTTPClient().fetch(HISTORY_API, request_timeout=10)
    except Exception as exc:  # noqa: BLE001
        log.warning("spark-lab: History Server unavailable, event logs of %s kept: %s",
                    username, exc)
        return []
    ids = []
    for app in json.loads(resp.body):
        if any(a.get("sparkUser") == username for a in app.get("attempts", [])):
            if _APP_ID.match(app.get("id", "")):
                ids.append(app["id"])
    log.info("spark-lab: %d event log(s) to delete for %s", len(ids), username)
    return ids


async def _run_silo_cleanup(core, username: str, app_ids: list, log):
    pod_name = f"silo-cleanup-{username}"
    steps = [
        'export MC_HOST_local="http://$SILO_ACCESS_KEY:$SILO_SECRET_KEY@'
        'silo.spark-lab.svc:9000"',
        f'mcli rm --recursive --force "local/spark-data/users/{username}/" || true',
    ]
    # Spark 4.1 writes rolling event logs as a directory eventlog_v2_<appId>/;
    # also remove single-file logs (<appId>, <appId>.zstd, *.inprogress).
    for app_id in app_ids:
        steps.append(f'mcli rm --recursive --force "{EVENT_LOG_PREFIX}/eventlog_v2_{app_id}/" || true')
        for suffix in ("", ".zstd", ".inprogress", ".zstd.inprogress"):
            steps.append(f'mcli rm --force "{EVENT_LOG_PREFIX}/{app_id}{suffix}" >/dev/null 2>&1 || true')
    cmd = "; ".join(steps)
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=pod_name, namespace=PLATFORM_NS),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[client.V1Container(
                name="mcli",
                image=SILO_IMAGE,
                image_pull_policy="IfNotPresent",
                command=["sh", "-c", cmd],
                env=[
                    client.V1EnvVar(name="SILO_ACCESS_KEY", value_from=client.V1EnvVarSource(
                        secret_key_ref=client.V1SecretKeySelector(
                            name=SILO_SHARED_SECRET, key="accessKey"))),
                    client.V1EnvVar(name="SILO_SECRET_KEY", value_from=client.V1EnvVarSource(
                        secret_key_ref=client.V1SecretKeySelector(
                            name=SILO_SHARED_SECRET, key="secretKey"))),
                ],
            )],
        ),
    )
    # Remove a leftover pod from a previous deletion, then create.
    try:
        await core.delete_namespaced_pod(pod_name, PLATFORM_NS)
    except client.ApiException:
        pass
    await _ignore_conflict(core.create_namespaced_pod(PLATFORM_NS, pod))
    log.info("spark-lab: started Silo cleanup pod for %s", username)


def spark_lab_pre_spawn_hook(spawner):
    """KubeSpawner pre_spawn_hook (runs before the Notebook Pod is created)."""
    return _provision_namespace(spawner.user.name, spawner.log)


class SparkLabSpawner(KubeSpawner):
    """KubeSpawner that also tears down the user's namespace and Silo data."""

    def load_state(self, state):
        # KubeSpawner persists the namespace in the Hub database and restores
        # it on every start, overriding user_namespace_template. A user created
        # before per-user namespaces (e.g. the admin `spark`, saved with
        # namespace spark-lab) would then be spawned outside user-<name>.
        # The namespace is the ownership boundary, so it must always be derived
        # from the user name. If the saved namespace differs, discard the whole
        # saved state: pod_name, dns_name and pvc_name all refer to objects in
        # the old namespace (a saved pvc_name also makes KubeSpawner skip
        # creating the PVC, leaving the new Pod Pending). The templates then
        # regenerate the names in user-<name>, as for a brand-new user.
        expected = f"user-{self.user.name}"
        saved = state.get("namespace")
        if saved and saved != expected:
            self.log.warning(
                "spark-lab: discarding saved spawner state of %s (namespace %s); using %s",
                self.user.name, saved, expected)
            state = {}
        # A saved pvc_name makes KubeSpawner assume the PVC exists and skip
        # creating it. Our PVC name is fixed by the template (claim-<user>), so
        # never trust the saved one: KubeSpawner then (re)creates the PVC if it
        # is missing and treats "already exists" as success.
        state = {k: v for k, v in state.items() if k != "pvc_name"}
        super().load_state(state)

    async def delete_forever(self):
        await _cleanup_user(self.user.name, self.log)
        return await super().delete_forever()


# --- wire everything into the Hub configuration -----------------------------
c.JupyterHub.spawner_class = SparkLabSpawner           # noqa: F821 (c is injected)
c.KubeSpawner.enable_user_namespaces = True            # noqa: F821
c.KubeSpawner.user_namespace_template = "user-{username}"  # noqa: F821
c.KubeSpawner.pre_spawn_hook = spark_lab_pre_spawn_hook    # noqa: F821
# Notebook servers run in user-<name> namespaces, where the chart's short
# hub URL (http://hub:8081) does not resolve. Use the fully qualified name.
c.JupyterHub.hub_connect_url = "http://hub.spark-lab.svc.cluster.local:8081"  # noqa: F821
