# **Silo PVC 存储架构与数据持久化总结**

## **1. 总体结论**

当前项目中的 Silo 使用 Kubernetes PVC 持久化数据。PVC 通过 Rancher Local Path Provisioner 动态创建 PV，实际数据存储在 Docker Desktop Kubernetes Node 的本地目录中。

**Silo 数据并不是存放在 Pod 容器内部，也不是直接挂载到 Mac Studio 的普通目录，而是存放在 Docker Desktop Node 使用的 Docker volume 中。**

因此，普通重启 Mac Studio、Docker Desktop 或 Node，按当前存储结构预期不会丢失数据。但如果删除底层 Docker volume、重置 Docker Desktop，或者误删 PVC 并触发存储回收，则存在数据丢失风险。

## **2. 存储结构**

**Mac Studio**

Docker Desktop 虚拟磁盘 Docker.raw

**Docker Named Volume**

846388d5f383d4a95231edc90baa994fd523f0410f0edbd60ca039284e680ed0

匿名 Docker volume，挂载到 Node 的 `/var`

**Kubernetes Node**

desktop-control-plane

**Local Path Provisioner**

/var/local-path-provisioner/

**Silo PVC / PV**

Silo Pod 内挂载到 `/data`

## **3. PVC、PV 和 StorageClass 配置**

根据实际 `kubectl` 输出：

| 配置项          | 当前值                                        |
| ------------ | ------------------------------------------ |
| Namespace    | `spark-lab`                                |
| PVC 名称       | `silo-data`                                |
| PVC 状态       | `Bound`                                    |
| PV 名称        | `pvc-3bd6717b-962e-48dd-beca-472f6d778e72` |
| 容量           | `5Gi`                                      |
| Access Mode  | `ReadWriteOnce`                            |
| StorageClass | `standard`                                 |
| Provisioner  | `rancher.io/local-path`                    |
| PV 类型        | `hostPath`                                 |
| Volume Mode  | `Filesystem`                               |
| PV 回收策略      | `Delete`                                   |
| Node 绑定      | `desktop-control-plane`                    |

实际 PV 数据路径：

```
/var/local-path-provisioner/
└── pvc-3bd6717b-962e-48dd-beca-472f6d778e72_spark-lab_silo-data/
```

PV 的关键配置：

```
spec:
  hostPath:
    path: /var/local-path-provisioner/pvc-3bd6717b-962e-48dd-beca-472f6d778e72_spark-lab_silo-data
    type: DirectoryOrCreate

  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - desktop-control-plane

  persistentVolumeReclaimPolicy: Delete
  storageClassName: standard
```

StorageClass 配置：

```
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
```

## **4. Docker Desktop 层面的持久化**

通过：

```
docker inspect desktop-control-plane --format '{{json .Mounts}}'
```

确认 Node 容器的 `/var` 挂载到了 Docker named volume：

```
Type:        volume
Name:        846388d5f383d4a95231edc90baa994fd523f0410f0edbd60ca039284e680ed0
Destination: /var
Driver:      local
RW:          true
```

通过：

```
docker volume inspect 846388d5f383d4a95231edc90baa994fd523f0410f0edbd60ca039284e680ed0
```

进一步确认：

- Driver 为 `local`。
- Scope 为 `local`。
- 具有独立的 Docker volume 名称。
- Label 为 `com.docker.volume.anonymous`，说明它是匿名 volume。
- Mountpoint 位于 Docker Engine 的 `/var/lib/docker/volumes/.../_data`。

**重要意义：** Node 的 `/var` 位于 Docker volume 中，而不是仅存在于 Node 容器的临时可写层。因此，只要 Node 容器重建时继续使用原来的 Docker volume，原有存储目录就具备保留条件。

## **5. Silo 实际数据目录验证**

通过以下命令：

```
docker exec desktop-control-plane ls -lah \
  /var/local-path-provisioner/pvc-3bd6717b-962e-48dd-beca-472f6d778e72_spark-lab_silo-data
```

已经实际检查到：

```
total 16K
drwxrwxrwx 4 root root 4.0K Nov 20 14:16 .
drwxr-xr-x 5 root root 4.0K Nov 20 15:58 ..
drwxr-xr-x 7 root root 4.0K Nov 22 03:04 .minio.sys
drwxr-xr-x 6 root root 4.0K Nov 22 03:07 spark-data
```

其中：

| 目录            | 用途                    |
| ------------- | --------------------- |
| `.minio.sys/` | Silo/MinIO 内部系统数据和元数据 |
| `spark-data/` | 业务对象存储数据              |

这一步确认了 PV 对应的数据目录确实存在，而且已经有实际业务数据目录。

## **6. 不同操作对数据的影响**

| 操作                                   | 数据影响    | 判断依据                                                     |
| ------------------------------------ | ------- | -------------------------------------------------------- |
| `./stopcluster.sh`                   | 保留      | 项目脚本不删除 PVC                                              |
| 重启 Mac Studio                        | 预期保留    | Docker volume 位于 Docker Desktop 持久化存储中；项目 README 也明确记载保留 |
| 退出并重启 Docker Desktop                 | 预期保留    | 普通重启不会主动删除 Docker volume；README 明确记载保留                   |
| 重启 Node 容器                           | 预期保留    | 前提是继续挂载原 Docker volume                                   |
| 删除 Node 容器，但保留原 Docker volume        | 具备保留条件  | 新 Node 需要重新挂载原 volume                                    |
| 删除并重建 Node，但使用新 volume               | 无法访问原数据 | 原数据仍在旧 volume 中                                          |
| 删除 Silo PVC                          | 有数据丢失风险 | PV 和 StorageClass 的回收策略均为 `Delete`                       |
| `./stopcluster.sh --purge`           | 数据会被删除  | 项目脚本会删除 `spark-lab` namespace                            |
| Docker Desktop Reset / Clean up data | 有数据丢失风险 | 可能清除底层 Kubernetes 和 Docker volume 数据                     |

需要特别区分：

- 删除 Kubernetes Node 对象，不等于删除 Docker Node 容器。
- 删除 Docker Node 容器，不等于删除它使用的 Docker volume。
- 删除 Docker volume，才会直接威胁保存在该 volume 中的数据。

## **7. 当前风险与改进建议**

### **建议一：将 PV 回收策略改为 Retain**

当前配置：

```
persistentVolumeReclaimPolicy: Delete
```

建议修改：

```
kubectl patch pv pvc-3bd6717b-962e-48dd-beca-472f6d778e72 \
  -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
```

修改后，删除 PVC 时，PV 会进入保留状态，不再按 `Delete` 策略自动回收底层存储。

注意：`Retain` 不能防止手动删除目录、Docker volume 或重置 Docker Desktop。

### **建议二：建立独立于 Docker.raw 的备份**

使用项目已有的导出脚本：

```
scripts/silo-export.sh
```

将导出结果保存到 Mac Studio 的普通文件系统目录，例如：

```
/Users/zhourui/spark-backups/silo/
```

这样即使 Docker Desktop 虚拟磁盘发生损坏或被清理，仍然有独立的数据副本。

### **建议三：暂时不必迁移 Silo 存储架构**

当前 Docker named volume 已经提供了一层独立于 Node 容器可写层的持久化机制。

现阶段更值得优先处理的是：

1. 将 PV 回收策略调整为 `Retain`。
2. 建立定期 Silo 数据导出备份。
3. 避免误执行 Docker Desktop Reset、Clean up data 或删除底层 volume。

## **8. 最终结论**

当前架构可继续使用

当前 Silo 的存储结构是：

**Silo PVC → Local Path PV → Node hostPath → Docker named volume → Docker.raw → Mac Studio 磁盘。**

它可以满足日常开发环境的持久化需求。普通重启 Mac Studio、Docker Desktop，以及复用原 Docker volume 的 Node 重建，预期都能保留数据。

但它还不是独立于 Docker Desktop 的业务存储系统。Docker Desktop 虚拟磁盘或底层匿名 volume 被删除，仍可能导致 Silo 数据丢失。

因此，建议保留当前架构，先调整 PV 回收策略，并增加 Mac 本地独立备份。