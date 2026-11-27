# Handoff：Spark2026 多用户改造（2026-11-23）

**用途：** 下一次会话从这里继续。先读本文件，再读 `docs/spark_usermanagement_design.md`（已批准的设计与验收标准）。
**当前位置（2026-11-23 更新）：** 多用户改造 **Phase 1、2、3 全部完成并通过验收**（Phase 2：13/13，Phase 3：14/14，见设计文档 §7）。`README.md`、`solution_design.md` §16 已更新为多用户版本。剩下的只有 Phase 4（可选的后续增强），未经用户要求不要开始。
**Git：** `b4a6860 phase1`、`56aa9d0 phase2` 已由用户提交。**Phase 3 的改动尚未提交。**

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

## 4. Phase 2 完成情况（2026-11-23）

Phase 1 留下的"断开"已全部修复：

- Client Mode：`spark.driver.host=$SPARK_LAB_DRIVER_HOST`（Downward API Pod IP），Driver/BlockManager 随机端口，`spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE`。`jobs/wordcount_executor.py`、01/02/03 Notebook 已更新；一个 Pod 中多个 Driver 互不冲突。
- Cluster Mode：`--conf spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-runner`，Silo 凭证用 `spark.kubernetes.driver.secretKeyRef.SILO_ACCESS_KEY=silo-shared-credentials:accessKey`（完整命令见 `tests/phase2_acceptance.sh` 的 2.3）。
- Spark UI：Portal nginx（`portal/default.conf.template`）按 URL 中的用户名转发：`/spark-ui/<user>/` → `spark-ui.user-<user>:4040`，`/spark-driver/<user>/` → `spark-driver-ui.user-<user>:4040`。Portal 页面增加了"查看谁的 Spark UI"输入框。旧的 Ingress、Middleware、`k8s/20-*`、`k8s/21-*` 已删除。
- 数据：`startcluster.sh` 上传公共样例 `s3a://spark-data/shared/wordcount/sample.txt`；示例输出写到 `s3a://spark-data/users/<name>/`（删除用户时一并删除）。
- 验收脚本：`tests/phase2_acceptance.sh [--cleanup]`，辅助函数 `tests/lib.sh`（用 bash 运行，不要在 zsh 里 source）。

Phase 2 中发现的要点：
- 跨 namespace 后 Notebook 必须用 FQDN 连 Hub：`c.JupyterHub.hub_connect_url = "http://hub.spark-lab.svc.cluster.local:8081"`。否则单用户服务器启动即失败，Hub 30 秒后删除 Pod。
- Docker Desktop 的 kindnet **不执行** NetworkPolicy（deny-all egress 实测仍连通）。
- 后台运行脚本时 `kubectl exec -i` 会被 SIGTTIN 挂起，已在 `scripts/common.sh` 加 `</dev/null`。
- 用户配额 `requests.memory` 改为 8Gi（Cluster Mode 需要约 6.1Gi）。

**仍未更新（文档类，计划在 Phase 3 之后统一做）：** `README.md`（仍是单用户、DummyAuthenticator、`spark-client`、`/spark-ui/` 的旧说明）和 `solution_design.md`。

## 5. Phase 3 完成情况与后续

- History Server：`/history/`，`k8s/60-history-server.yaml`；所有应用的 event log 写到 `s3a://spark-data/spark-logs/`（`images/spark-defaults.conf`）。
- Headlamp：`/headlamp/`，仅管理员，登录 token 用 `scripts/headlamp-token.sh` 生成（只读 `view`，不含 Secret）。
- 删除用户时同时删除该用户的 event log（Hub 通过 History Server REST API 按 `sparkUser` 查找）。
- Portal 增加了 History Server、Headlamp 卡片。
- 验收：`tests/phase3_acceptance.sh [--cleanup]`；回归：`tests/phase2_acceptance.sh --cleanup`。

Phase 4（可选，未开始）：每用户 Silo 凭证与策略、Spark UI 鉴权（jupyter-server-proxy）、限制 Hub 权限的 ValidatingAdmissionPolicy、TLS、资源用量统计。NetworkPolicy 在本环境无效（kindnet 不执行），如需要得先换 CNI。

Phase 3 之后修复：管理员 `spark`（单用户时代的旧账号）启动 Notebook 失败，原因是 Hub 数据库中保存的旧 Spawner 状态（namespace `spark-lab`、`pvc_name`）。`SparkLabSpawner.load_state` 已处理，详见设计文档 §7 "Phase 3 之后发现的问题"。新用户不受影响。

已记录、暂不实施：依图风格的人脸抓拍流水线实验，见 `docs/face_pipeline_experiment_design.md`。

已知的小问题：Phase 2 验收的 2.5（Cluster Mode Driver UI 跳转）曾两次在连续删建用户后失败，单独复现和之后的三次完整运行都通过；测试已加诊断输出，如再失败会打印最后的状态码和 Driver Pod 状态。

## 6. 环境与运维速查

| 项 | 值 |
|---|---|
| 启动 / 停止 | `./startcluster.sh`（幂等，约 4 秒到 1 分钟）/ `./stopcluster.sh`（`--purge` 删除全部数据） |
| 管理员 | `spark`，密码在 `.lab/credentials.env` 的 `JUPYTER_PASSWORD`（git-ignored） |
| Silo root | `.lab/credentials.env` 的 `SILO_ACCESS_KEY`/`SILO_SECRET_KEY`（仅 Console 管理员与脚本用） |
| Silo 共享用户 | `lab-user`，`.lab/credentials.env` 的 `SILO_SHARED_*`，集群内 `spark-lab/silo-shared-credentials` |
| 当前集群状态 | `spark-lab`：hub、proxy、silo、portal 运行中；`traefik` 运行中；只有管理员的 `user-spark` namespace（测试用户 alice/bob 已由 `--cleanup` 删除）；NativeAuthenticator 中只有 `spark`；Silo 中只有 `shared/wordcount/sample.txt` 和单用户时代留下的旧对象（`input/`、`run-*`、`notebook-demo/`、`cluster-wordcount/`，未删除） |
| 用户 namespace 批量清理 | `kubectl delete ns -l spark-lab/tenant=true` |
| 镜像导入节点 | `startcluster.sh` 用临时特权 Pod `kube-system/spark-lab-image-loader` 执行 `ctr images import`（kind 模式节点有独立 containerd） |
| Hub 日志 | `kubectl -n spark-lab logs deploy/hub`（hook 日志前缀 `spark-lab:`） |
| History Server / Headlamp | `kubectl -n spark-lab logs deploy/spark-history` / `kubectl -n headlamp logs deploy/headlamp` |

## 7. 回滚（如需要）

回滚点：`b4a6860 phase1`、`56aa9d0 phase2`。建议把 Phase 3 的改动也提交一次。回滚步骤见 `docs/spark_usermanagement_design.md` §8。
