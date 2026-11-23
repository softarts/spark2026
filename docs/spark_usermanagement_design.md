# Spark2026 多用户用户管理设计

**文档类型：** 设计与实施计划（练手项目，只做用户管理的最小子集）
**状态：** Phase 1–3 已实施并通过验收（2026-11-23）。所有决定和逐项核实的结论汇总于此，取代之前的分计划、审查和 H1 分析文档。
**核实说明：** 文中所有"已核实"的结论都来自对当前集群的只读检查或 server-side dry-run（不创建资源）。集群为 Docker Desktop Kubernetes v1.36.1，单节点 `desktop-control-plane`（同时是控制平面），节点容量 18 CPU / 16 GiB。

---

## 1. 目标

1. 用户在 `/hub/signup` 注册，管理员 `spark` 在 `/hub/authorize` 批准后才能登录。每个用户有独立的用户名和密码。
2. 每个用户的 Notebook、Driver、Executor 都在各自的 namespace `user-<name>` 中，使用各自的 ServiceAccount `spark-runner`，在 Kubernetes RBAC 层面不能操作其他用户的 namespace。
3. **namespace 即资源归属**：一个 Pod 在 `user-alice` 中，就属于 Alice。这由 RBAC 保证，不依赖 label。
4. 用户的 Notebook 不再拿到 Silo 的 root 凭证。
5. 管理员能看到所有用户的 Pod（用 Headlamp 按 namespace 查看）。
6. 用户不能通过特权 Pod 提权（Pod Security baseline）。

目标是用户管理的最小子集，不是企业级多租户平台。

## 2. 已接受的范围与风险

| 项目 | 决定 | 含义 |
|---|---|---|
| Pod Security | **做 baseline**（§5） | 堵住特权 Pod 提权（§4）。这是唯一一项从"不做"改为"做"的决定 |
| NetworkPolicy | 不做 | Pod 之间网络互通，用户可以访问彼此的 Spark UI 和 RPC 端口。属于信息泄露，不是提权。类比 Jenkins：所有人都能看到所有作业。另外已实测（2026-11-23）：Docker Desktop 的 kindnet **不执行** NetworkPolicy（deny-all egress 策略下仍能连通），所以即使配置了策略在本环境也不生效 |
| Silo 凭证 | 所有用户共享一个非 root 凭证 | 用户之间可以读写彼此的对象，目录前缀只是约定，不是隔离 |
| 管理员进入用户服务器 | 接受 | `spark` 是 JupyterHub 管理员，保留 `admin_access: true`，可进入任何用户的 Notebook |
| Spark UI 鉴权 | 不做 | 所有人都能看到所有作业。jupyter-server-proxy 推迟到 Phase 4 |
| 现有 `spark` 用户数据 | 不迁移，直接删除 `spark-lab/claim-spark` 和 `jupyter-spark` | |
| namespace 初始化 | H1（Hub 的 `pre_spawn_hook` 自动完成，§6） | 不引入新组件 |

剩余风险：Hub 拥有集群范围权限，Hub 被攻破等于集群被攻破；baseline 仍允许容器以 root 运行，容器逃逸需要内核漏洞。以上在练手项目中接受。

---

## 3. 设计

### 3.1 认证

- NativeAuthenticator（hub 镜像中已有 1.3.0）：`enable_signup=True`、`open_signup=False`、`admin_users={spark}`。去掉 DummyAuthenticator 和 `allowed_users`。
- **防止 `spark` 被抢注**：已核实 NativeAuthenticator 的 `create_user` 逻辑会把用户名在 `admin_users` 中的注册者直接标记为已批准的管理员。因此 `startcluster.sh` 在 Hub 就绪后立即用 `/hub/signup` 注册 `spark`，密码随机生成写入 `.lab/credentials.env`；已存在时跳过。在这之前不开放注册页。

### 3.2 每用户 namespace 与归属

- KubeSpawner 7.1.0：`enable_user_namespaces=True`、`user_namespace_template="user-{username}"`。已核实它只创建 namespace（`_ensure_namespace`，409 忽略），不创建 SA / Role / RoleBinding，且启用后 Hub 需要集群范围的 ClusterRole、reflector 跨所有 namespace watch。
- 其余资源由 `pre_spawn_hook` 补齐（§6）。
- namespace 的 label：`spark-lab/tenant=true`、`spark-lab/user=<name>`，以及 §5 的 Pod Security 标签。

### 3.3 RBAC

| 身份 | 范围 | 权限 |
|---|---|---|
| Hub（`spark-lab/hub`） | 集群范围 ClusterRole | namespaces：get、create、delete、**patch**（patch 用于给已存在的 namespace 补 PSA 标签）；pods、persistentvolumeclaims、events：get、list、watch、create、delete；serviceaccounts、services、secrets、resourcequotas、rolebindings：get、create、patch；对 ClusterRole `spark-runner` 的 `bind`（resourceNames 限定） |
| `user-<name>/spark-runner` | 只在自己的 namespace（ClusterRole + RoleBinding） | pods、pods/log、services、configmaps：get、list、watch、create、delete、deletecollection、patch；persistentvolumeclaims：get、list、delete、deletecollection（**不能 create**）；events：get、list、watch。**不含 secrets，不含 namespaces** |
| Executor Pod | — | namespace 的 `default` SA，无任何 RBAC 权限 |

- `spark-runner` 中**绝不能出现 `namespaces` 资源**：通过 RoleBinding 授予的 namespaces 权限可以作用于该 namespace 对象本身，使用户能改自己 namespace 的 PSA 标签。已核实当前 `spark-client` 对 `patch/update/create namespaces` 都是 no，Phase 1 验收要用 `user-alice/spark-runner` 再测一次。
- 去掉 `create persistentvolumeclaims`：Phase 1 不用 Spark on-demand PVC，只保留 stop 时的 list / delete / deletecollection。
- 已核实 `spark-client` 无 `pods/exec`、`pods/ephemeralcontainers` 权限。

### 3.4 Spark

- Notebook 环境变量（KubeSpawner 模板展开）：
  - `SPARK_USER={username}`：Spark UI 和 History Server 显示真实用户名，而非 `jovyan`；
  - `SPARK_LAB_NAMESPACE=user-{username}`；
  - `SPARK_LAB_DRIVER_HOST`：Downward API 取 `status.podIP`。
- Client Mode：`spark.driver.host` 设为 Pod IP，Driver 和 BlockManager 用随机端口。同一 Pod 中多个 Driver（多个 kernel/进程）互不冲突。删除 `spark-client-driver` 这个 headless Service。
- Cluster Mode：`spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE`、`spark.kubernetes.authenticate.driver.serviceAccountName=spark-runner`。
- 示例代码读取上述环境变量，仍放在镜像中（公共、只读来源），每次启动复制到用户 `work/examples/`。

### 3.5 Silo（共享凭证）

- `startcluster.sh` 用 `mcli` 创建非 root 用户 `lab-user`，策略为只能读写 bucket `spark-data`。凭证存于 `spark-lab/silo-shared-credentials`，由 hook 复制到每个用户 namespace。
- 目录约定（前缀不是隔离）：

```text
s3a://spark-data/shared/        公共样例数据
s3a://spark-data/users/<name>/  各用户输出
s3a://spark-data/spark-logs/    Event log（Phase 3）
```

- Silo root 凭证只用于 Silo 本身、Console 管理员登录，以及 `startcluster.sh` 和 `silo-export.sh`。

### 3.6 Spark UI 路由（不鉴权）

| 路径 | 后端 |
|---|---|
| `/spark-ui/<user>/` | `spark-ui.user-<user>.svc:4040` |
| `/spark-driver/<user>/` | `spark-driver-ui.user-<user>.svc:4040` |

- 在 Portal nginx 中加一条正则 location，按 URL 中的用户名动态反代，设置 `X-Forwarded-Context`。所有用户共用一条静态规则，不为每个用户建 Ingress。
- 删除 `k8s/50-ingress.yaml` 中旧的 `/spark-ui`、`/spark-driver` 规则和 `k8s/21-cluster-driver-ui-service.yaml`。

### 3.7 删除用户：删除其全部数据

在 `/hub/admin` 先 Stop Server，再 Delete User。Hub 调用 Spawner 的 `delete_forever` 和 Authenticator 的 `delete_user`，在其中加清理逻辑：

| 对象 | 处理 | 机制 |
|---|---|---|
| namespace `user-<name>`（PVC、所有 Pod、SA、RoleBinding、Secret、Service、Quota） | 删除 | 重写 `delete_forever`（默认只删 PVC），用 Hub 已有的 `kubernetes_asyncio`（已核实存在）删除整个 namespace |
| Silo `users/<name>/` | 删除 | Hub 镜像无 S3 客户端（已核实无 boto3/botocore），由 Hub 在 `spark-lab` 启动一次性 `mcli` Pod，以 `lab-user` 执行 `mcli rm --recursive --force` |
| 用户的 Spark event log（Phase 3 后） | 删除 | 日志文件名是 application ID，不含用户名；清理 Pod 先经 History Server REST API 按 `sparkUser` 找出应用 ID 再删 |
| NativeAuthenticator 账号记录 | 删除 | 1.3.0 自带 `delete_user`（已核实源码） |
| JupyterHub 用户记录 | 删除 | JupyterHub 自带 |
| 公共样例（镜像、Silo `shared/`） | 保留 | 不属于任何用户 |

限制：`users/<name>/` 只是约定，用户也能写到别的前缀；`sparkUser` 可伪造。两者都属已接受的共享凭证风险。

---

## 4. 安全分析：特权 Pod 提权

### 4.1 结论

**用户能通过特权 Pod 获得远超自身 token 的权限，直到 cluster-admin。这是真实漏洞。** RBAC 只决定 token 能调用哪些 API，而特权或 hostPath Pod 拿到的是节点 root；节点上存放着其他身份的凭证，这些不受用户 token 的 RBAC 限制。

### 4.2 证据（2026-11-22，只读 / dry-run，未创建资源）

| # | 检查 | 结果 |
|---|---|---|
| 1 | 以 `system:serviceaccount:spark-lab:spark-client` 用 `kubectl create --dry-run=server` 提交 `privileged: true`、`hostPID: true`、`hostPath: /` 的 Pod | **API Server 接受**（`privileged=true hostPath=/`），因 namespace 无 PSA 标签 |
| 2 | 节点角色 | 唯一节点 `desktop-control-plane` 同时是控制平面 |
| 3 | 控制平面 static pod 主机路径 | kube-apiserver 挂 `/etc/kubernetes/pki`（含 CA 私钥），controller-manager 挂 `/etc/kubernetes/controller-manager.conf` |
| 4 | Hub token 节点位置 | `/var/lib/kubelet/pods/<hub-uid>/volumes/kubernetes.io~projected/kube-api-access-*/token` |

只用 Spark 参数即可做到第 1 项：Spark 4.1 支持 `spark.kubernetes.executor.volumes.hostPath.*` 和 `podTemplateFile`（已在 jar 中核实）。

### 4.3 提权路径（原理，未执行）

```text
Alice（Notebook token，只能操作 user-alice）
  └─ 在 user-alice 创建 privileged + hostPath:/ 的 Pod（RBAC 允许 create pods）
       └─ Pod 内取得节点 root
            ├─ /etc/kubernetes/pki/ca.key   → 自签 system:masters 证书 = cluster-admin
            ├─ /etc/kubernetes/*.conf        → 控制平面凭证
            ├─ /var/lib/kubelet/.../token     → 所有 Pod 的 SA token（含 Hub、Bob）
            └─ containerd socket             → 进入任意容器
```

没有 Pod Security 时，§1 目标 2（RBAC 层面不能操作其他用户 namespace）不成立。问题不在 RBAC，而在 Pod 规格中的特权字段不受 RBAC 管辖，必须由准入控制限制。

### 4.4 修复选择

采用 **Pod Security baseline**（Kubernetes 1.36 内置准入，无需新组件），见 §5。它拒绝 privileged、hostPath、hostPID、hostNetwork、hostIPC、hostPort 和额外 capabilities，无论请求来自 Spark 参数、pod template 还是 curl。备选的镜像白名单（ValidatingAdmissionPolicy 只允许 `spark-lab-runtime` 镜像）留待 Phase 4。

---

## 5. Pod Security（baseline）逐项核实

验证方式：`kubectl label --dry-run=server ns spark-lab pod-security.kubernetes.io/enforce=<level>`，API Server 对该 namespace 中正在运行的 Pod 逐个评估并列出违规者，不写入标签。

| # | 事项 | 核实结果 | 结论 / 调整 |
|---|---|---|---|
| 1 | K8s 版本与 PSA 状态 | 服务端 v1.36.1；dry-run 返回 `baseline:v1.36` 评估，说明 PodSecurity 准入已默认启用；所有 namespace 当前无 PSA 标签 | 通过 |
| 2 | 用户 namespace 设 `enforce=baseline` | `user_namespace_labels` 只在**新建**时生效，已存在的 namespace 不会更新 | hook 还要对已存在的 namespace 执行一次 patch；Hub 需 `namespaces: patch` |
| 3 | 固定 policy version | `enforce-version=v1.36` 被接受 | 采用 `enforce=baseline`+`enforce-version=v1.36`，`warn=restricted`、`audit=restricted`（均 v1.36）。升级 K8s 时人工评估 |
| 4 | 用户不能改 namespace 安全标签 | `spark-client` 对 `patch/update/create namespaces` 全部 no | 通过。前提：`spark-runner` 不含 `namespaces`（§3.3）。Phase 1 用 `user-alice/spark-runner` 复测 |
| 5 | PSA exemptions | apiserver 只有 `--enable-admission-plugins=NodeRestriction`，无 `--admission-control-config-file`，无任何豁免 | 通过。无标签的 namespace（`spark-lab`、`kube-system` 等）不受约束 |
| 6 | root initContainer、Notebook、Driver、Executor | baseline 下**仅 `jupyter-spark` 违规**，原因是 Z2JH 默认的 init container **`block-cloud-metadata`**（`privileged: true` + `NET_ADMIN`，来自 `singleuser.cloudMetadata.blockWithIptables: true`）。`prepare-home`（root）、`notebook`、Driver、Executor 均不违规 | **阻断项，必须修复**：设 `singleuser.cloudMetadata.blockWithIptables: false`。该 init container 用于封锁云厂商元数据服务 169.254.169.254，Docker Desktop 无此服务，关闭不失保护 |
| 7 | privileged / hostPath / hostNetwork / hostPID / hostIPC 被拒 | 需真正加标签后测 | Phase 1 验收：以 `user-alice/spark-runner` dry-run 逐项提交 privileged、hostPath、hostNetwork、hostPID、hostIPC、hostPort、`capabilities.add:[SYS_ADMIN]`，均须被拒 |
| 8 | Spark 不能经 `volumes.hostPath.*` 绕过 | Phase 1 验收 | 预期：① 仅 Executor 设 hostPath → Driver 可建、Executor 被 403、作业挂起（Spark 重试，不立即失败，需用户 `stop()`）；② Cluster Mode Driver 设 hostPath → `spark-submit` 直接失败；③ `podTemplateFile` 指定 privileged → 同 ①②。dry-run 不创建 Pod |

补充：
- `restricted` 只能作 warn/audit，不能 enforce（dry-run 显示 Notebook、Portal、Driver、Executor 在 restricted 下都违规：`allowPrivilegeEscalation != false`、`runAsNonRoot != true`、`seccompProfile` 等）。
- PSA 只在创建/更新 Pod 时检查，不驱逐已运行的违规 Pod。hook 在启动 Notebook 前加标签，不受影响。
- baseline 仍允许容器 root 与默认 capabilities，逃逸需内核漏洞（已接受风险）。
- 平台自身：`startcluster.sh` 的镜像导入步骤会在 `kube-system` 建临时特权 Pod，使用 Mac 的 admin kubeconfig，不在 `user-*` namespace，不受 baseline 影响。可选地给 `spark-lab` 也加 baseline（删除旧 `jupyter-spark` 后，Hub、Proxy、Silo、Portal 均符合）。

---

## 6. H1：namespace 初始化

### 6.1 方案对比与选择

| 做法 | 谁初始化 | 例子 | 取舍 |
|---|---|---|---|
| 共用一个 namespace | 不需要 | Z2JH 默认 | 最简单，用户间无 K8s 隔离 |
| **声明式控制器（业界最佳实践）** | 独立控制器，依据 CR/事件生成并持续调和 | Kubeflow Profiles、Kyverno generate、Capsule、CML per-user namespace | 权限分离、自动纠偏、可审计；需运维新组件 |
| **H1：Spawner hook（已选）** | Hub 在 spawn 前用 `pre_spawn_hook` 创建 | KubeSpawner `enable_user_namespaces`+hook | 不引入新组件，逻辑约 60 行在 values.yaml；Hub 权限变大，只在 spawn 时补齐 |
| H2：管理员脚本 | Mac 上执行 | — | Hub 权限最小，但每批准一人跑一次 |

H1 不是最佳实践，但改动最小、不引入外部依赖。演进路径：日后可把 hook 的"确保对象存在"逻辑迁到 Kyverno generate 或 Profile 风格控制器，用户 namespace 结构不变。

### 6.2 `pre_spawn_hook` 步骤（每次 spawn 前执行，幂等）

1. KubeSpawner 创建 namespace `user-<name>`，带 §3.2 和 §5 的 label。
2. 在该 namespace 确保以下对象存在（409 改 patch）：

| 对象 | 说明 |
|---|---|
| ServiceAccount `spark-runner` | Notebook 与 Cluster Mode Driver 使用 |
| RoleBinding `spark-runner` | 引用 ClusterRole `spark-runner`，仅本 namespace 生效 |
| Secret `silo-shared-credentials` | 从 `spark-lab` 复制的非 root 凭证 |
| Service `spark-ui` | selector `component=singleuser-server`，端口 4040 |
| Service `spark-driver-ui` | selector `spark-role=driver`，端口 4040 |
| ResourceQuota `user-quota` | 只限制 requests：`requests.cpu: 4`、`requests.memory: 8Gi`、`pods: 12`、`persistentvolumeclaims: 2`（只限 requests，避免拒绝 Spark 默认无 cpu-limit 的 Pod。8Gi 的依据：Notebook 2G + Cluster Mode Driver 约 1.4Gi + 2 个 Executor 各约 1.4Gi ≈ 6.1Gi，6Gi 不够） |

3. 任一步失败则抛异常，Notebook 不启动；用户在 spawn 页、管理员在 Hub 日志看到原因。

### 6.3 局限

- 资源只在 spawn 时补齐，手动删除后要等下次 spawn 才恢复。
- 改 ClusterRole `spark-runner` 对所有用户立即生效（RoleBinding 引用同一个 ClusterRole）。
- 删除仍在运行服务器的用户，JupyterHub 会拒绝，需先 Stop Server。

---

## 7. 阶段与验收

验收全部用真实用户身份完成：Notebook 中挂载的 token 调 `SelfSubjectReview`（确认身份）和 `SelfSubjectAccessReview`（确认权限），特权 Pod 用 server dry-run，不用管理员 kubeconfig。

### Phase 1：认证、namespace、Pod Security

改动：`jupyterhub/values.yaml`（authenticator、user namespaces、`pre_spawn_hook`、`delete_forever`、环境变量、`cloudMetadata.blockWithIptables: false`）、`k8s/10-spark-rbac.yaml`（改为 ClusterRole `spark-runner`）、新增 `k8s/11-hub-rbac.yaml`、`startcluster.sh`（预注册 spark、建 Silo `lab-user`），删除旧 `jupyter-spark` 和 `claim-spark`。

| # | 测试 | 预期 |
|---|---|---|
| 1.1 | `spark` 用 credentials.env 密码登录，开 `/hub/admin`、`/hub/authorize` | 通过 |
| 1.2 | alice 注册后直接登录 | 拒绝（待批准） |
| 1.3 | spark 批准后 alice 登录 | 通过 |
| 1.4 | alice Notebook 位置与身份 | `user-alice`；`system:serviceaccount:user-alice:spark-runner` |
| 1.5 | alice 在 user-alice / user-bob 建 pods | yes / no |
| 1.6 | alice get secrets（两 namespace）、create clusterrolebindings、在 user-bob create serviceaccounts/token、patch 自己 namespace | 全部 no |
| 1.7 | alice Pod 环境变量含 Silo root 凭证 | 没有 |
| 1.8 | 特权/hostPath/hostNetwork/hostPID/hostIPC/hostPort/SYS_ADMIN Pod（dry-run）与 Spark hostPath | 全部被 baseline 拒绝；正常 Notebook/Client/Cluster 作业不受影响 |
| 1.9 | 删除 bob | user-bob namespace、`users/bob/`、账号记录消失；alice 不受影响 |

**Phase 1 实施结果（2026-11-22，已通过）**

全部测试以真实身份执行（alice 的 `SelfSubjectReview` 返回 `system:serviceaccount:user-alice:spark-runner`，RBAC 矩阵用 `--as` 该 SA）。

- 1.1–1.3：admin `spark` 登录并打开 `/hub/admin`；alice 注册后未批准登录被拒；`spark` 批准后 alice 登录成功。
- 1.4：alice 的 Notebook 在 `user-alice`，SA 为 `spark-runner`，token 身份核实无误。namespace 带 `pod-security.kubernetes.io/enforce=baseline`（v1.36）等全部标签。
- 1.5：alice 在 `user-alice` 可 create pods（yes），在 `user-bob`（no）。
- 1.6：get secrets（两 namespace）、create clusterrolebindings、在 user-bob create serviceaccounts/token、patch 自己 namespace —— 全部 no。
- 1.7：alice 的 `SILO_ACCESS_KEY=lab-user`（非 root）。
- 1.8：privileged、hostPath、hostNetwork、hostPID、hostIPC、hostPort、`SYS_ADMIN` 全部被 `violates PodSecurity "baseline:v1.36"` 拒绝；普通 Pod 通过（restricted 仅告警）。Spark hostPath 产生同样的 Pod 规格，准入层以相同规则拒绝（与客户端无关）。
- 1.9：通过 Hub Admin API Stop + Delete bob 后，`user-bob` namespace 与账号记录消失，`user-alice` 不受影响（返回 204）。

需要新镜像时，用户要重启 Notebook Server。关键修复：JupyterHub 5 的 `check_allowed` 默认拒绝，必须设 `Authenticator.allow_all: true`（授权由 NativeAuthenticator 的审批在 `authenticate()` 内完成）。

### Phase 2：Spark

改动：`images/spark-defaults.conf`、示例 Notebook 与 jobs、Portal nginx、`k8s/50-ingress.yaml`。

| # | 测试 | 预期 |
|---|---|---|
| 2.1 | alice、bob 同时跑 02（Client Mode） | Executor 分别在 user-alice / user-bob，计数正确 |
| 2.2 | alice 同 Pod 两个 kernel 各跑一个 Client Mode | 都成功（Pod IP + 随机端口） |
| 2.3 | alice 提交 Cluster Mode | Driver、Executor 都在 user-alice，Driver SA 为 spark-runner |
| 2.4 | alice 提交时指定 `spark.kubernetes.namespace=user-bob` | RBAC 拒绝（403） |
| 2.5 | `/spark-ui/alice/`、`/spark-ui/bob/`、`/spark-driver/alice/` | 都能开，跳转带正确前缀 |
| 2.6 | 03 S3A Notebook | 读 `shared/`，写 `users/alice/` |

**Phase 2 实施结果（2026-11-23，已通过，13/13）**

验收脚本 `tests/phase2_acceptance.sh`（辅助函数 `tests/lib.sh`），全部操作都在 alice / bob 自己的 Notebook Pod 中、用各自的 `spark-runner` token 执行；`--cleanup` 结束时删除两个测试用户。

| # | 结果 |
|---|---|
| 2.1 | alice、bob 同时运行 02：Executor 分别位于 `user-alice`、`user-bob`（观察期间只出现这两个 namespace），计数正确 |
| 2.2 | alice 的同一个 Pod 中两个 Driver 进程各自拿到自己的 Executor（Pod IP + 随机端口，不冲突） |
| 2.3 | Cluster Mode：Driver 和 Executor 都在 `user-alice`，Driver SA 为 `spark-runner`；Executor 用共享凭证从 `shared/wordcount/` 读、向 `users/alice/cluster-wordcount/` 写，Driver 状态 Succeeded |
| 2.4 | alice 指定 `spark.kubernetes.namespace=user-bob`：`pods is forbidden: User "system:serviceaccount:user-alice:spark-runner" cannot create resource "pods" ... in the namespace "user-bob"`，user-bob 中没有创建任何对象 |
| 2.5 | `/spark-ui/alice/`、`/spark-ui/bob/` 各自显示本人的应用，跳转到 `/spark-ui/<user>/jobs/`，导航链接都带前缀；`/spark-driver/alice/` 同样正确 |
| 2.6 | 03 Notebook：local 模式由 `jupyter-alice` 读 Silo，k8s 模式由 Executor Pod 读 Silo；都读 `shared/`、写 `users/alice/` |
| PSA | Client Mode 设置 `spark.kubernetes.executor.volumes.hostPath.*`：Executor 创建被 `violates PodSecurity "baseline:v1.36": hostPath volumes` 拒绝（30 秒内重试 122 次），注册的 Executor 数为 0；与 §5 #8 的预期一致（作业挂起，需用户 `stop()`） |
| 清理 | 删除 alice、bob 后：两个 namespace、Silo `users/` 下的数据、NativeAuthenticator 记录全部消失，`shared/` 保留 |

实施中发现并修复的问题：

| 问题 | 现象 | 修复 |
|---|---|---|
| Notebook 连不上 Hub | Notebook 在 `user-<name>` 中，Hub 注入的 `JUPYTERHUB_API_URL=http://hub:8081` 是短名，跨 namespace 解析失败；单用户服务器启动即退出，Hub 30 秒后放弃并删除 Pod | `c.JupyterHub.hub_connect_url = "http://hub.spark-lab.svc.cluster.local:8081"`（`spark_lab_hook.py`） |
| Hub 入站策略 | chart 的 hub NetworkPolicy 默认只接受同 namespace 的 Notebook | `hub.networkPolicy.interNamespaceAccessLabels: accept`（本环境 kindnet 不执行策略，此项为可移植性） |
| 后台运行 `startcluster.sh` 卡住 | `kubectl exec -i` 读取终端被 SIGTTIN 停止 | `scripts/common.sh` 中该调用加 `</dev/null` |
| 配额不足 | 6Gi 容纳不了 Cluster Mode（Notebook + Driver + 2 Executor） | 提高到 8Gi，hook 对已有配额也执行 patch |

Phase 2 改动的文件：`jupyterhub/values.yaml`（`SPARK_LAB_DRIVER_HOST`、hub networkPolicy）、`jupyterhub/spark_lab_hook.py`（`hub_connect_url`、配额）、`jobs/wordcount_executor.py`、`jobs/test_s3a.py`、`notebooks/01–03`、`portal/default.conf.template`（新）、`portal/index.html`、`k8s/40-portal.yaml`、`k8s/50-ingress.yaml`、`startcluster.sh`（上传 `shared/wordcount/sample.txt`、发布 nginx 模板、清理旧路由对象）、`scripts/common.sh`、`tests/`（新）；删除 `k8s/20-client-driver-service.yaml`、`k8s/21-cluster-driver-ui-service.yaml`。

### Phase 3：History Server 与管理视图

| # | 内容 | 验收 |
|---|---|---|
| 3.1 | History Server：同一运行时镜像，在 `spark-lab` 运行，读 `s3a://spark-data/spark-logs/`，路由 `/history/`，共享不鉴权 | 作业结束后仍可查看，用户列显示 alice/bob |
| 3.2 | Headlamp：仅管理员，绑定内置 `view` ClusterRole（不含 Secret），路由 `/headlamp/`，`startcluster.sh` 打印限时 token | 管理员能按 `user-*` namespace 看每个用户的 Notebook/Driver/Executor |
| 3.3 | Portal 增加 History Server、Headlamp 入口及 `/spark-ui/<user>/` 说明 | — |

**Phase 3 实施结果（2026-11-23，已通过，14/14）**

验收脚本 `tests/phase3_acceptance.sh`（3.4 会故意删除 alice）。Phase 2 验收在 Phase 3 部署之后重跑，13/13 通过。

| # | 结果 |
|---|---|
| 3.1 | alice 的 Local、Client、Cluster Mode 应用和 bob 的 Local 应用都出现在 History Server，Spark User 分别为 alice、bob；Cluster Mode 命令行上没有任何 S3A 配置，event log 凭证完全来自 `spark-defaults.conf` 的 `secretKeyRef`；`/history/history/<应用ID>/jobs/` 返回 200，导航链接带前缀 |
| 3.2 | Headlamp：不带 token 访问 API 返回 403；`headlamp-viewer` token 能列出 `user-alice/jupyter-alice`、`user-alice/…-driver`、`user-bob/jupyter-bob`；读 Secret、删除 Pod 返回 403；Headlamp 自己的 ServiceAccount `can-i list pods -A` = no |
| 3.3 | Portal 有 `/history/`、`/headlamp/`、`/spark-ui/<用户>/`、`/hub/signup` 入口 |
| 3.4 | 删除 alice：她的 3 份 event log、`users/alice/`、命名空间全部删除；bob 的 event log 保留 |

实现要点：

| 项目 | 做法 |
|---|---|
| event log | `images/spark-defaults.conf`：`spark.eventLog.enabled=true`、`spark.eventLog.dir=s3a://spark-data/spark-logs/`。S3A 默认凭证改用 `EnvironmentVariableCredentialsProvider`（AWS SDK v2）：Notebook 通过 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` 环境变量拿到，Driver 和 Executor Pod 通过 `spark.kubernetes.{driver,executor}.secretKeyRef.*` 从本命名空间的 `silo-shared-credentials` 拿到 |
| event log 目录 | Spark 在目录不存在时拒绝启动，而 S3 上空前缀不算目录，所以 `startcluster.sh` 写入标记对象 `spark-logs/.keep` |
| event log 格式 | Spark 4.1 默认滚动日志：目录 `eventlog_v2_<应用ID>/`（含 `events_1_<应用ID>.zstd` 和 `appstatus_<应用ID>`），已实测 |
| History Server | `k8s/60-history-server.yaml`：同一运行时镜像，`start-history-server.sh` 前台运行，`spark.history.fs.update.interval=10s`；Traefik 去掉 `/history` 前缀并设置 `X-Forwarded-Context` |
| 删除用户的 event log | Hub 调 History Server REST API，找出 `sparkUser` 等于用户名的应用 ID（先校验格式），交给一次性 `mcli` Pod 删除 `eventlog_v2_<ID>/` 及单文件形式。清理 Pod 的镜像固定为与 Silo 相同的 digest（Phase 1 时写成了无 tag 的 `pgsty/silo`，会拉取 `:latest`） |
| Headlamp | chart 0.46.0，`config.baseURL=/headlamp`，关闭 chart 默认的 cluster-admin ClusterRoleBinding；登录 token 来自 `headlamp/headlamp-viewer`（ClusterRole `view`，不含 Secret），由 `scripts/headlamp-token.sh` 生成，默认 8 小时有效。与设计的差异：`startcluster.sh` 不直接打印 token，只提示命令，避免 token 留在终端历史和日志里 |

**Phase 3 之后发现的问题（2026-11-23，已修复）：管理员 `spark` 无法启动 Notebook**

| 项目 | 内容 |
|---|---|
| 现象 | `spark` 登录后 Spawn failed：`pods "jupyter-spark" is forbidden: error looking up service account spark-lab/spark-runner` |
| 原因 | KubeSpawner 把 `namespace`、`pod_name`、`dns_name`、`pvc_name` 保存在 Hub 数据库，每次启动用 `load_state` 恢复，覆盖 `user_namespace_template`。`spark` 是单用户时代建的账号，保存的是 `namespace: spark-lab`，所以 Pod 被建到 `spark-lab`（那里没有 `spark-runner`）。验收中的 alice、bob 都是新用户，状态为空，因此没测出来。去掉 namespace 后还有第二个问题：保存的 `pvc_name` 让 KubeSpawner 认为 PVC 已存在而跳过创建，新 Pod 卡在 Pending（`persistentvolumeclaim "claim-spark" not found`） |
| 修复 | `SparkLabSpawner.load_state`（`spark_lab_hook.py`）：保存的 namespace 不是 `user-<用户名>` 时丢弃全部保存状态；任何情况下都不采用保存的 `pvc_name`（名称由模板 `claim-<用户名>` 固定，KubeSpawner 会创建缺失的 PVC，已存在时视为成功） |
| 验证 | `spark` 的 Notebook 在 `user-spark` 中启动，SA `spark-runner`，PVC `claim-spark` 新建并绑定；停止再启动后文件仍在（同一个 PV，日志 `PVC claim-spark already exists`）；Phase 2 回归 13/13 |
| 影响 | `spark` 的旧 Notebook 主目录（`spark-lab/claim-spark`）在 Phase 1 已按决定删除，`user-spark` 中是一个新的空主目录 |

### Phase 4：以后

每用户 Silo 凭证、NetworkPolicy、jupyter-server-proxy（Spark UI 鉴权）、ValidatingAdmissionPolicy（约束 Hub、镜像白名单）、TLS、资源统计。

---

## 8. 回滚

- git 保存当前 `jupyterhub/values.yaml` 和 RBAC。回滚时 `helm upgrade` 回旧 values，恢复 DummyAuthenticator 与单 namespace 模式。
- 回滚后手动删除所有 `user-*` namespace：`kubectl delete ns -l spark-lab/tenant=true`。
- Silo `lab-user` 不影响 root 凭证，可保留。
