# Handoff：Spark2026 多用户改造（2026-11-23）

**用途：** 下一次会话从这里继续。先读本文件，再读 `docs/spark_usermanagement_design.md`（已批准的设计与验收标准）。
**当前位置：** 多用户改造 **Phase 1 已完成并通过验收**；**Phase 2 尚未开始，等待用户确认后再动手。**
**Git：** 所有改动都**未提交**（仓库只有 `63b0f93 initial commit`）。

---

## 1. 项目背景（一句话）

Mac（Apple Silicon）上的 Docker Desktop Kubernetes v1.36.1 单节点（`desktop-control-plane`，同时是控制平面，18 CPU / 16 GiB）上的 Spark 学习平台：Traefik 入口 `http://localhost:8080`（局域网可访问）→ Portal / JupyterHub / Silo Console；Spark 4.1.0 统一运行时镜像 `spark-lab-runtime:0.3.0`；Silo（MinIO 社区分支）作 S3A 对象存储。练手项目，用户管理只做最小子集。

总体设计：`solution_design.md`；使用说明：`README.md`（**尚未更新为多用户**，见 §4）。

## 2. 用户已做的决定（不要再次询问）

| 事项 | 决定 |
|---|---|
| 认证 | JupyterHub NativeAuthenticator：用户 `/hub/signup` 注册，管理员 `spark` 在 `/hub/authorize` 批准。不接外部身份源 |
| 隔离模型 | 每用户 namespace `user-<name>` + 每用户 SA `spark-runner`；namespace 即资源归属 |
| namespace 初始化 | H1：Hub 的 `pre_spawn_hook`（不是业界最佳实践，最佳实践是声明式控制器，已接受） |
| Pod Security | 用户 namespace enforce `baseline`（v1.36），warn/audit `restricted` |
| NetworkPolicy | 不做（Pod 网络互通，接受信息泄露风险） |
| Silo | 所有用户共享一个**非 root** 凭证 `lab-user`（仅限 bucket `spark-data`）；目录前缀不是隔离 |
| 管理员 | `spark` 是 JupyterHub admin，保留 `admin_access: true`（可进入用户服务器，已接受） |
| Spark UI | 不鉴权（类似 Jenkins，人人可见）；jupyter-server-proxy 放 Phase 4 |
| 删除用户 | 删除该用户全部数据（namespace、Silo `users/<name>/`、账号记录、以后的 event log） |
| 旧 `spark` 用户数据 | 不迁移，已删除 |

## 3. Phase 1 完成情况

### 3.1 改动的文件

| 文件 | 内容 |
|---|---|
| `jupyterhub/values.yaml` | NativeAuthenticator、`admin_users: [spark]`、`allow_all: true`、`open_signup: false`、`minimum_password_length: 8`；`singleuser.serviceAccountName: spark-runner`；`cloudMetadata.blockWithIptables: false`；环境变量 `SPARK_USER={username}`、`SPARK_LAB_NAMESPACE=user-{username}`、Silo 共享凭证 |
| `jupyterhub/spark_lab_hook.py`（新） | 由 `startcluster.sh` 用 `--set-file hub.extraConfig.sparkLabMultiuser=` 注入。开启 `enable_user_namespaces`；`pre_spawn_hook` 幂等创建 namespace（PSA + tenant 标签，已存在则 patch）、SA、RoleBinding、复制 Silo Secret、Service `spark-ui`/`spark-driver-ui`、ResourceQuota（只限 requests）、LimitRange；`SparkLabSpawner.delete_forever` 删除 namespace 并在 `spark-lab` 启动一次性 `mcli` Pod 删 `s3a://spark-data/users/<name>/` |
| `k8s/10-spark-rbac.yaml` | 改为 ClusterRole `spark-runner`（不含 secrets、namespaces、create pvc） |
| `k8s/11-hub-rbac.yaml`（新） | Hub 的 ClusterRole `spark-lab-hub` + ClusterRoleBinding（含对 `spark-runner` 的 `bind`） |
| `startcluster.sh` | 旧 credentials 文件自动补 `SILO_SHARED_*`；创建 Silo 用户 `lab-user` 与策略 `spark-data-rw`；写 `spark-lab/silo-shared-credentials`；删除单用户时代对象（`jupyter-spark`、`claim-spark`、`spark-client` SA/Role/RoleBinding、headless Service）；Hub 就绪后预注册 `spark`（防抢注） |
| `stopcluster.sh` | 停止所有 `user-*` namespace 的 Pod；`--purge` 删除 `spark-lab/tenant=true` 的 namespace |
| `docs/spark_usermanagement_design.md`（新） | 四份旧文档合并而成，含 Phase 1 实施结果 |

### 3.2 验收结果（全部通过，真实身份）

1.1 admin 登录 / 1.2 未批准被拒 / 1.3 批准后登录 / 1.4 `user-alice` + `system:serviceaccount:user-alice:spark-runner` / 1.5 跨 namespace 建 Pod 被拒 / 1.6 secrets、clusterrolebinding、token、改 namespace 全部被拒 / 1.7 Notebook 拿到 `lab-user` 不是 root / 1.8 privileged、hostPath、hostNetwork、hostPID、hostIPC、hostPort、SYS_ADMIN 全部被 `baseline:v1.36` 拒绝 / 1.9 删除 bob 后其 namespace 与账号记录消失，alice 不受影响。

测试脚本在 `/tmp/p1test.sh`（**临时目录，可能已被清除**；重写时注意：spawn 用 `GET /hub/spawn`；Hub API 用 cookie 认证时须带 `?_xsrf=<cookie 中 _xsrf 的值>`）。

### 3.3 实施中发现的要点

- JupyterHub 5 的 `check_allowed` 默认拒绝，必须 `Authenticator.allow_all: true`，审批由 NativeAuthenticator 在 `authenticate()` 内完成。
- Z2JH 默认的 `block-cloud-metadata` init 容器是特权容器，会被 baseline 拒绝，所以关闭了。
- `pre_spawn_hook` 在 `spawner.start()` **之前**执行，所以 hook 必须自己创建 namespace（不能依赖 KubeSpawner 的 `_ensure_namespace`）。
- Hub 镜像有 `kubernetes_asyncio 36.1.0`，没有 boto3，所以 Silo 清理用一次性 `mcli` Pod。

## 4. 已知"断开"的地方（Phase 1 有意留下，Phase 2 修复）

Phase 1 删除了单用户时代的 `spark-client` SA 和 headless Service `spark-client-driver`，并把 Notebook 移到 `user-<name>`。以下内容**目前不能用**：

| 位置 | 问题 |
|---|---|
| `jobs/wordcount_executor.py`、`notebooks/02_wordcount_executor.ipynb`、`notebooks/03_wordcount_s3a.ipynb`（`EXECUTORS="k8s"`） | 仍用 `spark-client` SA、`spark-client-driver.spark-lab.svc`、namespace `spark-lab`，Client Mode 会失败 |
| README 中的 Cluster Mode `spark-submit` 示例 | 仍写 `--conf spark.kubernetes.namespace=spark-lab`、`serviceAccountName=spark-client` |
| `k8s/50-ingress.yaml` 的 `/spark-ui`、`/spark-driver` | 指向已删除的 `spark-client-driver` 和 `spark-lab` 中的 Service，不再有效 |
| `k8s/21-cluster-driver-ui-service.yaml` | `startcluster.sh` 仍会 apply 它（`spark-lab` 中已无 Driver），Phase 2 删除 |
| `k8s/20-client-driver-service.yaml` | 文件仍在仓库，`startcluster.sh` 只用它做删除，Phase 2 删除文件 |
| `README.md`、`portal/index.html`、`solution_design.md` | 仍描述 DummyAuthenticator / 单用户 / `jupyter-spark` |

仍然可用：登录与审批、per-user Notebook、Local Mode（01 Notebook、`wordcount_local.py`）、`03` 的 `EXECUTORS="local"`、Silo Console、`scripts/silo-export.sh`。

## 5. 下一步：Phase 2（待用户确认后开始）

设计见 `docs/spark_usermanagement_design.md` §3.4、§3.6，验收见 §7 Phase 2。要做的事：

1. 环境变量：加 `SPARK_LAB_DRIVER_HOST`（Downward API `status.podIP`）。注意 Z2JH `singleuser.extraEnv` 支持 `valueFrom.fieldRef`。
2. Client Mode：`spark.driver.host=$SPARK_LAB_DRIVER_HOST`，Driver/BlockManager 随机端口；`spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE`；SA `spark-runner`。更新 `wordcount_executor.py`、02、03 Notebook。
3. Cluster Mode：`--conf spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-runner`；Silo 凭证改用 `silo-shared-credentials` 的 `secretKeyRef`。
4. Spark UI：Portal nginx 正则 location `/spark-ui/<user>/` → `spark-ui.user-<user>.svc.cluster.local:4040`，`/spark-driver/<user>/` → `spark-driver-ui...`，设置 `X-Forwarded-Context`（nginx 需要 `resolver` 指向 kube-dns 才能动态解析）。删除 `50-ingress.yaml` 中旧规则、`k8s/20-*`、`k8s/21-*`。`spark.ui.proxyRedirectUri /` 已在镜像 `images/spark-defaults.conf` 中。
5. 数据目录约定：样例输入 `s3a://spark-data/shared/`，用户输出 `s3a://spark-data/users/<name>/`（03 Notebook 目前写 `notebook-demo/<user>/`，需改）。
6. 修改 `images/`、`jobs/`、`notebooks/` 后 `startcluster.sh` 会重建镜像（约 25 秒），用户需重启 Notebook Server。
7. 按 §7 Phase 2 验收 2.1–2.6，特别是 2.2（同一 Pod 两个 kernel 各一个 Client Mode Session）和 2.4（指定 `user-bob` namespace 被 403）。Spark hostPath 绕过的端到端验证（Executor 403、作业挂起）也在 Phase 2 跑一次真实作业确认。

之后：Phase 3（History Server、仅管理员的 Headlamp、Portal 入口），然后更新 `README.md`、`portal/index.html`、`solution_design.md`。

## 6. 环境与运维速查

| 项 | 值 |
|---|---|
| 启动 / 停止 | `./startcluster.sh`（幂等，约 4 秒到 1 分钟）/ `./stopcluster.sh`（`--purge` 删除全部数据） |
| 管理员 | `spark`，密码在 `.lab/credentials.env` 的 `JUPYTER_PASSWORD`（git-ignored） |
| Silo root | `.lab/credentials.env` 的 `SILO_ACCESS_KEY`/`SILO_SECRET_KEY`（仅 Console 管理员与脚本用） |
| Silo 共享用户 | `lab-user`，`.lab/credentials.env` 的 `SILO_SHARED_*`，集群内 `spark-lab/silo-shared-credentials` |
| 当前集群状态 | `spark-lab`：hub、proxy、silo、portal 运行中；`traefik` 运行中；没有 `user-*` namespace（测试用户 alice/bob 已清理）；NativeAuthenticator 中只有 `spark`（已授权） |
| 用户 namespace 批量清理 | `kubectl delete ns -l spark-lab/tenant=true` |
| 镜像导入节点 | `startcluster.sh` 用临时特权 Pod `kube-system/spark-lab-image-loader` 执行 `ctr images import`（kind 模式节点有独立 containerd） |
| Hub 日志 | `kubectl -n spark-lab logs deploy/hub`（hook 日志前缀 `spark-lab:`） |

## 7. 回滚（如需要）

`git` 中只有 initial commit 的版本，单用户版（DummyAuthenticator + `spark-lab` namespace）从未提交，无法直接从 git 恢复。**继续 Phase 2 之前，建议先把当前工作区提交一次**，作为 Phase 1 的回滚点。回滚步骤见 `docs/spark_usermanagement_design.md` §8。
