# JupyterHub 用户存储：PVC 创建与挂载

本文记录本项目中 JupyterHub 用户主目录的存储创建、挂载和底层分配方式，并区分用户 PVC 与 Silo 的对象存储。

## 结论

- `singleuser.storage.homeMountPath` 是容器里的挂载点，不负责创建卷。
- 本项目通过 JupyterHub Helm Chart 的 `singleuser.storage.type: dynamic` 启用动态用户存储。用户启动 Notebook 时，KubeSpawner 为其创建或复用 PVC，并把 PVC 挂载到 `/home/jovyan`。
- 用户 PVC 位于该用户自己的 namespace 中。例如，用户 `spark` 使用 `user-spark/claim-spark`。
- 本项目 Docker Desktop 集群记录中的 `standard` StorageClass 使用 `rancher.io/local-path`，将 PV 对应到 Kubernetes 节点上的目录；它不是 Silo bucket。
- Silo 自己的数据也使用 PVC，但 Notebook 用户通过 S3 API/S3A 访问 Silo 对象，而不是把 Silo 的 `/data` 目录挂到用户 Pod。

## 从配置到挂载

仓库配置在 [`jupyterhub/values.yaml`](../jupyterhub/values.yaml)：

```yaml
singleuser:
  storage:
    type: dynamic
    capacity: 5Gi
    homeMountPath: /home/jovyan
    dynamic:
      storageClass: standard
```

各项职责：

| 配置 | 作用 |
| --- | --- |
| `type: dynamic` | 使用动态 PVC，而不是不持久化存储或预先手动创建的 PVC。 |
| `capacity: 5Gi` | 每个用户的 PVC 请求容量。 |
| `dynamic.storageClass: standard` | 指定用哪个 StorageClass 满足 PVC。 |
| `homeMountPath: /home/jovyan` | 将 PVC 挂到 Notebook 容器中的路径。 |

Chart 将这些 values 转换为 Hub/KubeSpawner 配置。创建 Notebook 时，KubeSpawner 在用户 namespace 中确保 PVC 存在，然后在 Notebook Pod 的 volume 和 `volumeMounts` 中引用它。它不是仓库里一个静态 PVC YAML；实际 PVC 是运行时创建的。项目的 PVC 命名为 `claim-<username>`，例如 `user-spark/claim-spark`。

本项目的启动钩子 `spark_lab_pre_spawn_hook` 会在 Notebook Pod 启动前运行，并确保 `user-<username>` namespace 及其他配套资源存在；它不把 `homeMountPath` 当作存储配置，也不是 PVC 的静态 manifest。用户 namespace 和 PVC 的相关处理见 [`jupyterhub/spark_lab_hook.py`](../jupyterhub/spark_lab_hook.py)。

`startcluster.sh` 中的 `--set-file hub.extraConfig.sparkLabMultiuser=jupyterhub/spark_lab_hook.py` **不是让 KubeSpawner 直接执行这个文件**。Helm 的 `--set-file` 把文件内容设为该 chart value；JupyterHub Chart 将 `hub.extraConfig` 作为 Hub 的额外 Python 配置。Hub 进程加载配置时会执行其中的顶层配置代码，注册 `SparkLabSpawner` 和 `pre_spawn_hook`；当用户启动 Notebook 时，JupyterHub 再调用这个 hook，随后 KubeSpawner 执行它的 spawn 流程。`pre_spawn_hook` 的时机和 PVC 创建是两个不同步骤。

```text
点击启动 Notebook
  -> JupyterHub 调用 pre_spawn_hook
  -> spark_lab_hook.py 创建/修正 user-<username> namespace 和配套资源
  -> KubeSpawner 在该 namespace 确保 claim-<username> PVC 存在
  -> Notebook Pod 引用 PVC，并挂载到 /home/jovyan
```

停止 Notebook 服务器通常不会删除用户 namespace 和 PVC，因此再次启动可以继续使用原有主目录。项目的 `SparkLabSpawner.delete_forever()` 会删除整个用户 namespace，PVC 也随之删除；PV 的后续处理由 StorageClass 的回收策略决定。

## `standard` 实际把数据放在哪里

StorageClass 的名称只是 PVC 选择存储类别的键；实际由 StorageClass 的 `provisioner` 和参数决定如何创建 PV。根据本项目记录的 Docker Desktop 集群配置，`standard` 的 provisioner 是 `rancher.io/local-path`，PV 使用节点本地 `hostPath`。Silo 的 PVC 记录也显示同一 StorageClass 和 provisioner，见 [`docs/design_silo_pvc_20261122.md`](./design_silo_pvc_20261122.md)。

记录中的 Silo 存储链路如下：

```text
Silo Pod 的 /data
  -> PVC spark-lab/silo-data
  -> StorageClass standard / rancher.io/local-path
  -> Kubernetes 节点目录 /var/local-path-provisioner/<PVC 对应目录>
  -> Docker Desktop Kubernetes 节点所在的 Linux 虚拟机
```

用户主目录 PVC 使用同一个 `standard` StorageClass，因此在这套集群记录中也由 local-path 提供，而不是落在 Silo bucket 里。集群设置可能变化；应以集群里的 StorageClass 和实际 PV 为准。可检查：

```bash
kubectl get storageclass standard -o yaml
kubectl -n user-spark get pvc
kubectl -n user-spark describe pvc claim-spark
kubectl get pv
kubectl get pv <PV 名称> -o yaml
```

注意：在本项目记录的 local-path 配置中，`5Gi` 是 PVC 的容量请求；该 provisioner 不强制限制节点目录的实际用量。实际可用空间受 Docker Desktop 虚拟机磁盘空间等因素约束。StorageClass 的回收策略也应以当前集群配置为准；记录中的 `standard` 为 `Delete`，删除 PVC 后相应 PV/目录可能被回收，不应把它当作备份。

## 用户 PVC 与 Silo 不是同一个卷

Silo 的 `spark-lab/silo-data` PVC 是给 Silo 服务 Pod 使用的后端数据目录。Silo 在该目录上提供 S3 兼容对象存储 API；Notebook 和 Spark 使用 Silo endpoint 以及对象存储凭证访问 bucket/prefix。`spark-data/users/<username>/` 这样的路径是对象存储里的 key 前缀，不是可直接交给 Kubernetes StorageClass 的 POSIX 目录。

因此：

- 用户的 Jupyter 文件和主目录：使用挂到 `/home/jovyan` 的 PVC。
- Spark 输入、结果和其他对象：使用 Silo 的 S3/S3A 接口。
- 不能只把 `storageClass` 改成 `silo` 就把 Silo bucket 变成用户 PVC。
- 若要让用户主目录的数据实际保存在 Silo 对象存储中，需要额外部署能把 S3 兼容后端呈现为文件系统卷的方案（例如相应的 CSI/文件系统层及其元数据服务），并确认其 POSIX 语义、并发行为、权限和回收策略符合 Jupyter 用户目录的需求。

## 相关仓库位置

- [`jupyterhub/values.yaml`](../jupyterhub/values.yaml)：动态存储类型、容量、StorageClass、挂载点及初始化容器。
- [`startcluster.sh`](../startcluster.sh#L194)：安装/升级 JupyterHub Chart，并通过 `--set-file` 传入 Hub Python 配置。
- [`jupyterhub/spark_lab_hook.py`](../jupyterhub/spark_lab_hook.py)：启动前创建用户 namespace；删除用户时清理 namespace。
- [`README.md`](../README.md)：说明用户 PVC 中的 Notebook 示例文件以及 Silo 数据访问方式。
- [`docs/design_silo_pvc_20261122.md`](./design_silo_pvc_20261122.md)：记录 Docker Desktop 集群中的 `standard`、local-path PV 和底层数据路径。

## 官方文档

- [Zero to JupyterHub: `hub.extraConfig`](https://z2jh.jupyter.org/en/stable/resources/reference.html#schema-hub-extraconfig)：Chart 如何把额外 Python 配置交给 Hub。
- [Helm `upgrade` 的 `--set-file`](https://helm.sh/docs/helm/helm_upgrade/)：`--set-file key=path` 将文件内容作为指定 values 值。
- [KubeSpawner 配置参考](https://jupyterhub-kubespawner.readthedocs.io/en/latest/spawner.html)：KubeSpawner 的 PVC 命名、存储和 Pod 配置选项。
- [JupyterHub Spawner API](https://jupyterhub.readthedocs.io/en/stable/reference/api/spawner.html)：Spawner 及其 spawn hooks 的定义。
- [Kubernetes StorageClasses](https://kubernetes.io/docs/concepts/storage/storage-classes/)：StorageClass 通过 `provisioner` 等字段决定动态 PV 的供应方式。
- [Kubernetes Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)：PVC/PV 的绑定、访问模式和回收概念。
