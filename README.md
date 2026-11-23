# Spark Platform Learning Lab

个人 Apache Spark 交互式实验平台，运行在 Mac 上的 **Docker Desktop Kubernetes (arm64)**，支持多个用户。用户在浏览器中注册，管理员批准后登录 JupyterHub，在自己的 Notebook 中运行 PySpark。实验内容：观察 Local、Kubernetes Client、Kubernetes Cluster 三种模式下 Driver 与 Executor 的位置和生命周期，通过 Hadoop S3A 读写 Silo 对象存储，并在 History Server 中查看已结束的作业。

- 一条命令启动：`./startcluster.sh`
- 一个入口：`http://localhost:8080/`（其他机器用 `http://<Mac 的局域网 IP>:8080/`），不需要 `kubectl port-forward`
- 每个用户的 Notebook、Spark Driver、Executor 都运行在自己的 Kubernetes 命名空间 `user-<用户名>` 中

设计背景见 [solution_design.md](solution_design.md)，多用户设计与验收记录见 [docs/spark_usermanagement_design.md](docs/spark_usermanagement_design.md)。

---

## 1. 快速开始

### 1.1 前提

| 要求 | 检查方式 |
| :--- | :--- |
| Docker Desktop，已启用 Kubernetes（Settings → Kubernetes → Enable Kubernetes） | `kubectl --context docker-desktop get nodes` |
| `kubectl`、`helm`（v3 或 v4）、`openssl`、`python3` | `helm version --short` |
| 首次运行需要联网：拉取 Helm chart、基础镜像和 Maven 上的 S3A JAR | |

脚本始终使用 `--context docker-desktop`，不会切换你当前的 kubectl context。

### 1.2 启动

```bash
./startcluster.sh
```

脚本是幂等的，可以反复执行。依次完成：

1. Docker Desktop 未运行时自动启动，并等待 Kubernetes API 就绪；
2. 构建统一运行时镜像 `spark-lab-runtime:0.3.0`，并导入 Kubernetes 节点（见 §8.2）。`images/`、`jobs/`、`notebooks/` 没有变化时跳过；
3. 第一次运行时生成本地凭证文件 `.lab/credentials.env`（已在 `.gitignore` 中）；
4. 部署平台命名空间 `spark-lab`、RBAC、Silo；创建 Bucket `spark-data`、共享的非 root Silo 用户 `lab-user`、公共样例数据和 event log 目录；部署 Spark History Server；
5. 发布入口页面和 Spark UI 反向代理（Portal）；
6. 安装 Traefik Ingress Controller（Mac 的 8080 端口）和 Headlamp；
7. 安装或升级 JupyterHub；
8. 等待 `http://localhost:8080/hub/health` 返回 200，预先注册管理员 `spark`，然后打印访问地址和凭证。

实测耗时：Docker Desktop 已运行且没有改动时约 6 秒；Docker Desktop 未运行时约 1 分钟；修改 `images/`、`jobs/` 或 `notebooks/` 后需要重建镜像，约 25 秒。

输出示例：

```text
Spark Platform Learning Lab is up.

  Portal (all links):  http://localhost:8080/
  From the LAN:        http://192.168.3.105:8080/

  Admin login:         user=spark  password=<随机生成>
  New users:           self-register at /hub/signup, then admin approves at /hub/authorize
  History Server:      http://localhost:8080/history/
  Headlamp (admin):    http://localhost:8080/headlamp/  (login token: scripts/headlamp-token.sh)
  Silo Console login:  accessKey=silo-admin  secretKey=<...>
  (stored in .lab/credentials.env)
```

### 1.3 停止

```bash
./stopcluster.sh            # 停止所有 Pod，保留全部数据
./stopcluster.sh --purge    # 卸载全部组件并删除所有数据（需要输入 spark-lab 确认）
```

| | `./stopcluster.sh` | `./stopcluster.sh --purge` |
| :--- | :--- | :--- |
| 所有用户的 Notebook、Driver、Executor Pod（运行中的 kernel 和未保存的修改会丢失） | 删除 | 删除 |
| Hub、Proxy、Silo、Portal、History Server、Traefik、Headlamp | 副本数缩到 0 | 卸载 |
| 用户命名空间 `user-*`（含各自的 Notebook 主目录 PVC） | **保留** | **删除** |
| Silo 对象、Hub 数据库（含所有账号） | **保留** | **删除** |
| Docker Desktop | 继续运行 | 继续运行 |

停止后再次运行 `./startcluster.sh` 即可恢复。

> 停止期间 Docker Desktop 仍然占用 Mac 的 8080 端口（Traefik 的 LoadBalancer Service 还在），只是没有服务响应。需要释放端口时，使用 `--purge` 或退出 Docker Desktop。

---

## 2. 用户与账号

认证使用 JupyterHub 的 **NativeAuthenticator**：本地用户名和密码，不依赖任何外部身份源。

| 操作 | 谁 | 地址 |
| :--- | :--- | :--- |
| 注册账号 | 任何人 | `http://localhost:8080/hub/signup`（密码至少 8 位） |
| 批准或撤销账号 | 管理员 `spark` | `http://localhost:8080/hub/authorize`（点击用户名切换批准状态） |
| 登录 | 已批准的用户 | `http://localhost:8080/hub/login` |
| 查看用户、启停或删除用户的服务器、删除用户 | 管理员 | `http://localhost:8080/hub/admin` |
| 修改自己的密码 | 用户 | `http://localhost:8080/hub/change-password` |

- **管理员 `spark`** 由 `startcluster.sh` 在 Hub 启动后立即注册，密码随机生成并保存在 `.lab/credentials.env` 的 `JUPYTER_PASSWORD`。必须预先注册，因为 NativeAuthenticator 会把注册用户名属于管理员列表的人直接批准为管理员。
- 未批准的用户登录时提示用户名或密码无效。
- 管理员可以进入任何用户的 Notebook（`admin_access: true`，练手项目接受）。

**用户启动 Notebook 时，平台自动完成以下工作**（Hub 的 `pre_spawn_hook`，`jupyterhub/spark_lab_hook.py`）：

| 在命名空间 `user-<用户名>` 中 | 作用 |
| :--- | :--- |
| 命名空间本身，带标签 `pod-security.kubernetes.io/enforce=baseline` | 拒绝特权、hostPath、hostNetwork、hostPID 等 Pod，防止用户借此拿到节点 root |
| ServiceAccount `spark-runner` 和 RoleBinding（ClusterRole `spark-runner`） | Notebook 和 Spark Driver 用它创建 Executor Pod，只在本命名空间有效，不能读 Secret |
| Secret `silo-shared-credentials` | 共享的非 root Silo 凭证 `lab-user`（只能访问 bucket `spark-data`） |
| Service `spark-ui`、`spark-driver-ui` | 供 Portal 转发该用户的 Spark UI |
| ResourceQuota：requests 最多 4 CPU / 8Gi 内存、12 个 Pod | 防止一个用户占满节点 |

**删除用户**（`/hub/admin` 中先 Stop Server，再 Delete User）会删除该用户的全部数据：命名空间 `user-<用户名>`（Notebook 主目录、所有 Pod），Silo 中的 `s3a://spark-data/users/<用户名>/`，该用户的 Spark event log，以及账号记录。公共样例数据不受影响。

---

## 3. 统一入口（Portal）

打开 `http://localhost:8080/`。每个服务一张卡片，并显示当前是否可达。Spark UI 卡片上方有一个输入框，填入用户名后链接指向该用户的 Spark UI。

| 路径 | 后端 | 何时可用 |
| :--- | :--- | :--- |
| `/` | Portal（nginx） | 始终 |
| `/hub/`、`/user/<用户名>/` | JupyterHub | 始终 |
| `/spark-ui/<用户名>/` | 该用户 Notebook Pod 的 4040 端口：Local 或 Client Mode 的 Driver | 该用户的 SparkSession 存活期间 |
| `/spark-driver/<用户名>/` | 该用户正在运行的 Cluster Mode Driver Pod 的 4040 端口 | Driver Pod 处于 Running 期间 |
| `/history/` | Spark History Server | 始终；应用结束约 10 秒后出现 |
| `/headlamp/` | Headlamp Kubernetes UI | 始终；需要管理员 token 登录 |
| `/silo/` | Silo Web Console | 始终；需要 Silo 管理员密钥 |

### 3.1 实现方式

```text
浏览器（本机或局域网）
   │  http://<host>:8080
   ▼
Docker Desktop 在 Mac 所有网卡上监听 *:8080     ← Traefik 的 LoadBalancer Service
   ▼
Traefik v3.7（k8s/50-ingress.yaml）
   ├── /hub, /user                 → JupyterHub proxy → 各用户 Notebook（user-<name>/jupyter-<name>）
   ├── /, /spark-ui, /spark-driver → Portal nginx（portal/default.conf.template）
   │      /spark-ui/<u>/...      → spark-ui.user-<u>.svc:4040
   │      /spark-driver/<u>/...  → spark-driver-ui.user-<u>.svc:4040
   ├── /history                    → spark-history.spark-lab.svc:18080
   ├── /headlamp                   → headlamp.headlamp.svc:80
   └── /silo                       → silo.spark-lab.svc:9001
```

- **为什么选 Traefik：** 社区版 ingress-nginx 已于 2026 年 3 月退役，不再有版本和安全修复（[Traefik 迁移文档](https://doc.traefik.io/traefik/migrate/nginx-to-traefik/)）。
- **为什么局域网可以访问：** Docker Desktop 把 `type: LoadBalancer` 的 Service 发布在 Mac 的所有网卡上（实测 `*:8080`）。如果打开 macOS 防火墙，需要允许 Docker 接受入站连接。
- **Spark UI 的子路径：** 代理去掉前缀，并发送请求头 `X-Forwarded-Context`（如 `/spark-ui/alice`），Spark 据此生成页面链接。Spark 自己的 302 跳转（如 `/` → `/jobs/`）要靠镜像中 `spark-defaults.conf` 的 `spark.ui.proxyRedirectUri /` 才会带前缀。History Server 用同样的机制。
- **Portal 如何找到用户的 Spark UI：** nginx 用正则从网址取出用户名，拼成 `spark-ui.user-<用户名>.svc.cluster.local`，通过集群 DNS 实时解析。新用户无需改任何配置。
- **Silo Console 子路径：** 环境变量 `MINIO_BROWSER_REDIRECT_URL=http://localhost:8080/silo`。Console 不检查主机名，局域网 IP 也能登录。

### 3.2 Spark UI 的限制

- **不要点 `http://jupyter-<用户名>:4040`。** `sc.uiWebUrl` 和 Notebook 中 `spark` 对象显示的 "Spark UI" 链接都是集群内地址，浏览器解析不了。示例 Notebook 会打印正确地址 `http://localhost:8080/spark-ui/<用户名>/`。
- `/spark-ui/<用户名>/` 只转发 4040 端口。同一个 Notebook Pod 中的第二个 SparkSession（例如另一个 kernel）使用 4041 端口，不会被转发；这时请看 History Server。
- 运行中的 UI 随 Driver 一起消失；作业结束后到 `/history/` 查看。
- 同一用户同时运行多个 Cluster Mode Driver 时，`/spark-driver/<用户名>/` 会在它们之间轮流转发。
- **Spark UI 和 History Server 都不鉴权**：局域网内任何人都能看到所有用户的作业，并能用 UI 上的 kill 按钮终止运行中的作业（练手项目接受，类似 Jenkins）。

---

## 4. 登录 Notebook，运行示例

1. 在 `http://localhost:8080/hub/signup` 注册，等管理员在 `/hub/authorize` 批准。
2. 登录后 JupyterHub（KubeSpawner）在命名空间 `user-<用户名>` 中创建 Pod `jupyter-<用户名>`，进入 JupyterLab。
3. 示例位于 `work/examples/`：

```text
work/examples/
├── notebooks/
│   ├── 01_wordcount_local.ipynb      Local Mode
│   ├── 02_wordcount_executor.ipynb   Kubernetes Client Mode（Executor Pods）
│   └── 03_wordcount_s3a.ipynb        读写 Silo（S3A）；可切换 Local / Executor Pods
└── jobs/
    ├── wordcount_local.py            Local Mode，终端运行
    ├── wordcount_executor.py         Client Mode，终端运行
    ├── wordcount.py                  Cluster Mode（spark-submit），可选读写 S3A
    └── test_s3a.py                   Local Mode 读写 Silo（文本 + Parquet）
```

**示例目录每次启动 Notebook Server 时都会从镜像重新复制。** 要修改示例，先复制到 `work/` 下的其他位置。

Notebook 中可用的环境变量（`jupyterhub/values.yaml`）：

| 变量 | 值 | 用途 |
| :--- | :--- | :--- |
| `SPARK_USER` | 用户名 | Spark UI 和 History Server 中显示的 Spark User |
| `SPARK_LAB_NAMESPACE` | `user-<用户名>` | `spark.kubernetes.namespace` |
| `SPARK_LAB_DRIVER_HOST` | 本 Pod 的 IP（Downward API） | Client Mode 的 `spark.driver.host` |
| `SPARK_LAB_RUNTIME_IMAGE` | `spark-lab-runtime:0.3.0` | Executor / Driver 镜像 |
| `SILO_ENDPOINT`、`SILO_ACCESS_KEY`、`SILO_SECRET_KEY` | Silo 地址和共享凭证 `lab-user` | 示例中显式配置 S3A |
| `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` | 同一份共享凭证 | `spark-defaults.conf` 的 S3A 默认凭证和 event log |

### 4.1 示例是如何进入 Notebook 的

示例与 Silo 无关：`.ipynb` 文件在用户自己的 PVC 上，Silo 中只有 Spark 写入的数据。

```text
jobs/ + notebooks/（本仓库）
   │  docker build（images/Dockerfile: COPY）
   ▼
镜像内 /opt/spark-lab/examples/
   │  Notebook Pod 的 init container "prepare-home"（jupyterhub/values.yaml）
   ▼
user-<用户名>/claim-<用户名> PVC 中的 /home/jovyan/work/examples/   ← JupyterLab 中看到的就是这里
```

init container 以 root 运行，还会把 `/home/jovyan` 的属主改为 UID 1000（local-path 存储类忽略 `fsGroup`，新 PVC 默认属于 root）。Pod Security baseline 允许 root，只禁止特权等字段。

镜像中的 `/opt/spark-lab/examples/jobs/` 也存在于 Cluster Mode 的 Driver Pod 中，所以 `spark-submit` 可以直接用 `local:///opt/spark-lab/examples/jobs/wordcount.py`。

### 4.2 `01_wordcount_local.ipynb`：Local Mode

`master("local[*]")`：Driver JVM 由 Notebook kernel 通过 Py4J 启动，所有 task 在这个 JVM 的线程中执行，不创建 Executor Pod。task 读取的主机名都是 `jupyter-<用户名>`；Python worker 的 PID 与 kernel PID 不同，因为 RDD lambda 运行在 Driver JVM 派生的 Python worker 进程里。

### 4.3 `02_wordcount_executor.ipynb`：Kubernetes Client Mode

- **Driver：** 这个 Notebook 的 kernel，位于 `user-<用户名>/jupyter-<用户名>`。
- **Executor：** Driver 用 ServiceAccount `spark-runner` 在**同一命名空间**中创建 2 个 Pod。指定别的命名空间会被 RBAC 拒绝（403）。
- **回连：** Executor 连接 `spark.driver.host` = 本 Pod 的 IP，端口随机。所以同一个 Pod 中的多个 kernel 可以各自运行 Client Mode Session，互不冲突（已实测）。

在 Mac 上另开终端观察：

```bash
kubectl -n user-<用户名> get pods -l spark-role=executor -w
```

浏览器打开 `http://localhost:8080/spark-ui/<用户名>/executors/`。task 读取的主机名就是 Executor Pod 名，Spark REST API 返回的 `hostPort` 就是 Executor Pod 的 IP，两者一一对应。执行 `spark.stop()` 后 Driver 删除 Executor Pod；Executor 的 ownerReference 指向 Notebook Pod，Notebook Pod 被删除时它们也会被回收。

资源配额：每个 Executor 申请 0.5 CPU、约 1.4Gi 内存（1g 堆 + overhead）。超出命名空间配额（8Gi）的 Executor 不会被创建。

### 4.4 终端：Local 和 Client Mode 脚本

在 JupyterLab 的 Terminal 中运行：

```bash
python3 ~/work/examples/jobs/wordcount_local.py
python3 ~/work/examples/jobs/wordcount_executor.py     # WORDCOUNT_EXECUTORS=1 可改 Executor 数
```

### 4.5 Cluster Mode（spark-submit）

在 JupyterLab 的 Terminal 中提交。Notebook 只负责提交，Kubernetes 在用户命名空间中创建独立的 Driver Pod：

```bash
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode cluster \
  --name wordcount-cluster \
  --conf spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-runner \
  --conf spark.kubernetes.container.image=$SPARK_LAB_RUNTIME_IMAGE \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.driverEnv.WORDCOUNT_HOLD_SECONDS=60 \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.executor.request.cores=500m \
  --conf spark.kubernetes.driver.request.cores=500m \
  local:///opt/spark-lab/examples/jobs/wordcount.py
```

- `local://` 指 **Driver Pod 镜像内** 的路径。
- `WORDCOUNT_HOLD_SECONDS=60` 让 Driver 完成后再保持 60 秒，这期间可打开 `http://localhost:8080/spark-driver/<用户名>/`。之后到 `/history/` 查看。
- Pod 变化：`…-driver` Running → Driver 创建 `…-exec-1/2` → 作业结束，Executor 被删除，Driver Pod 保留为 Completed，可以 `kubectl -n user-<用户名> logs <driver-pod>`。

**读写 Silo：** 只需加上输入输出路径，凭证由镜像的 `spark-defaults.conf` 通过 `secretKeyRef` 自动注入 Driver 和 Executor Pod（已实测，命令行上不出现任何密钥）：

```bash
  --conf spark.kubernetes.driverEnv.SILO_INPUT=s3a://spark-data/shared/wordcount/ \
  --conf spark.kubernetes.driverEnv.SILO_OUTPUT=s3a://spark-data/users/$SPARK_USER/cluster-wordcount/ \
```

#### Driver Pod 是怎么被创建的

```text
user-<用户名>/jupyter-<用户名> Pod
  ├─ spec.serviceAccountName: spark-runner        ← jupyterhub/values.yaml: singleuser.serviceAccountName
  ├─ /var/run/secrets/kubernetes.io/serviceaccount/  ← 自动挂载该 SA 的短期 token
  └─ spark-submit --master k8s://… --deploy-mode cluster
       │  Spark 内置的 Kubernetes 客户端（fabric8）读取上面的 token
       │  POST /api/v1/namespaces/user-<用户名>/pods  → <name>-driver Pod
       ▼
  API Server：RBAC（RoleBinding spark-runner → ClusterRole spark-runner，仅本命名空间）
            + Pod Security baseline（拒绝特权 / hostPath 等）
       ▼
<name>-driver Pod（SA 由 spark.kubernetes.authenticate.driver.serviceAccountName 指定，同样是 spark-runner）
  └─ Driver 用自己的 token 创建 Executor Pod（SA default，无任何权限）
```

`spark-runner` 不能读 Secret，Driver 和 Executor 中的 Silo 凭证是 Pod spec 里的 `secretKeyRef`，由 kubelet 在启动容器时注入。

### 4.6 `03_wordcount_s3a.ipynb`：在 Notebook 中读写 Silo

一步步演示：配置 S3A → 列出公共输入 → 读取并计算 WordCount → 写成 Parquet → 列出输出 → 读回 → 在 Silo Console 中查看。

- 输入：公共样例 `s3a://spark-data/shared/wordcount/`（`startcluster.sh` 上传）。
- 输出：`s3a://spark-data/users/<用户名>/wordcount-s3a/wordcount.parquet/`。
- 第一个代码单元格 `EXECUTORS = "local"`（默认）在 Notebook Pod 中读写；改为 `"k8s"` 由 Executor Pod 读写（输出中会显示 `read … from Silo on …-exec-1`）。

`01`、`02` 和 `wordcount_local.py` 的输入来自内存中的 `parallelize([...])`，结果只打印，**不写 Silo 数据**（它们只写 event log）。另一个读写 Silo 的脚本是 `test_s3a.py`，写到 `s3a://spark-data/users/<用户名>/test-s3a/run-<时间戳>/`。

---

## 5. 查看作业：History Server 与 Headlamp

### 5.1 Spark History Server（`/history/`）

每个 Spark 应用（Local、Client、Cluster Mode）的 Driver 都把 event log 写到 `s3a://spark-data/spark-logs/`（`images/spark-defaults.conf`：`spark.eventLog.enabled=true`）。History Server（`k8s/60-history-server.yaml`，同一个运行时镜像）每 10 秒扫描一次，重建每个应用的 Spark UI：Jobs、Stages、Executors、Environment 都能看。

- 列表中的 **Spark User** 就是 JupyterHub 用户名（来自 `SPARK_USER`）。这个值由平台设置，但用户可以自己改，所以它只用于展示，不是可信身份。
- 应用结束约 10 秒后出现在列表中。S3A 不支持在写入过程中刷新文件，所以运行中的应用要到结束时才有完整内容；运行中请看 `/spark-ui/<用户名>/`。
- 所有用户共享，不鉴权。
- 删除用户时，平台通过 History Server 的 REST API 找出 Spark User 等于该用户名的应用，删除对应的 event log。

已实测：alice 的 Local、Client、Cluster 三种模式的应用都显示为 Spark User = alice；`/history/history/<应用ID>/jobs/` 能打开，导航链接带 `/history/` 前缀；删除 alice 后她的 3 份 event log 被删除，bob 的保留。

### 5.2 Headlamp（`/headlamp/`，仅管理员）

Headlamp 是 Kubernetes SIG UI 维护的 Web 界面，也是已归档的 Kubernetes Dashboard 的官方继任者。在这里，管理员按命名空间查看谁在运行什么：`user-<用户名>` 中的 `jupyter-<用户名>` 是 Notebook，`*-driver` 是 Cluster Mode Driver，`*-exec-N` 是 Executor。可以查看 Pod 的 YAML、日志和事件。

登录：

```bash
scripts/headlamp-token.sh          # 在 Mac 上运行，打印一个 8 小时有效的 token（参数可改，如 30m）
```

把 token 粘贴到 `http://localhost:8080/headlamp/` 的登录框。

| 身份 | 权限 | 已验证 |
| :--- | :--- | :--- |
| Headlamp 自己的 ServiceAccount | 无（chart 默认的 cluster-admin 绑定已关闭） | `can-i list pods -A` = no |
| token 对应的 `headlamp/headlamp-viewer` | 内置 ClusterRole `view`：所有命名空间的只读访问，**不含 Secret** | 能列出 alice、bob 的 Pod；读 Secret、删 Pod 都返回 403 |
| 不带 token | 无 | API 请求返回 403 |

只有能在 Mac 上用管理员 kubeconfig 运行脚本的人才能拿到 token，所以 Headlamp 实际上只对管理员开放。

---

## 6. Silo 对象存储

### 6.1 Web 界面与凭证

Silo 内置完整的 Web Console（Silo `RELEASE.2026-11-12T00-00-00Z` 内嵌 Console v2.7.2）：`http://localhost:8080/silo/`，可以浏览 Bucket 和目录，预览、下载、上传、删除对象。

| 凭证 | 谁持有 | 用途 |
| :--- | :--- | :--- |
| root（`silo-admin`，`.lab/credentials.env` 的 `SILO_ACCESS_KEY`/`SILO_SECRET_KEY`） | 平台管理员 | Console 登录、`startcluster.sh`、`scripts/silo-export.sh`；**不会注入任何 Notebook 或 Spark Pod** |
| `lab-user`（`SILO_SHARED_*`，Secret `silo-shared-credentials`） | 所有用户共享 | Notebook、Driver、Executor、History Server。策略 `spark-data-rw`：只能访问 bucket `spark-data` |

S3 API 端点只在集群内提供：`http://silo.spark-lab.svc:9000`。

目录约定：

```text
s3a://spark-data/shared/          公共样例数据（startcluster.sh 上传）
s3a://spark-data/users/<用户名>/   各用户的输出；删除用户时删除
s3a://spark-data/spark-logs/      所有应用的 event log（History Server 读取）
```

**这只是约定，不是隔离。** 所有用户使用同一个 `lab-user` 凭证，能读写彼此的对象，包括 event log（练手项目接受；以后可改为每用户一个 Silo 用户和策略）。

### 6.2 存储是怎么挂载的

仓库中的 `data/` 目录**没有**挂载给 Silo；`data/silo-export/` 只是导出脚本的输出目录。

```text
Silo Pod 中的 /data
   │  volumeMount
   ▼
PVC spark-lab/silo-data（StorageClass standard，provisioner rancher.io/local-path）
   ▼
节点 desktop-control-plane 上的目录 /var/local-path-provisioner/pvc-<uid>_spark-lab_silo-data/
   │  节点是 Docker Desktop Linux 虚拟机里的一个容器（kind 模式）
   ▼
虚拟机磁盘镜像 ~/Library/Containers/com.docker.docker/Data/vms/0/data/Docker.raw → Mac 本地硬盘
```

用户的 Notebook 主目录（PVC `user-<用户名>/claim-<用户名>`）和 Hub 数据库（PVC `spark-lab/hub-db-dir`，含所有账号）使用同样的链路。

### 6.3 能否在 Mac 本地硬盘上看到实际的存储文件？

**不能直接看到。** 数据确实在 Mac 硬盘上，但有两层阻隔：

1. **数据在虚拟机磁盘镜像里**（`Docker.raw`），Finder 打不开。kind 节点没有挂载 Mac 的 `/Users`（实测 `hostPath` 报 `not a directory`）。
2. **Silo 使用 MinIO 的 xl 存储格式。** 每个对象是一个目录：小对象的数据内嵌在 `xl.meta` 中；大对象另有 `<uuid>/part.1`（实测 2,000,000 字节的对象对应 2,000,064 字节的 `part.1`，多出的是校验数据）。

查看方式：

| 方式 | 命令 / 地址 | 看到的内容 |
| :--- | :--- | :--- |
| Web Console | `http://localhost:8080/silo/` | 逻辑对象，可预览和下载 |
| 导出到 Mac | `scripts/silo-export.sh [bucket] [prefix]`，例如 `scripts/silo-export.sh spark-data users/alice/` | 普通文件，写到 `./data/silo-export/<bucket>/...`（导出的 Parquet 可以直接用 DuckDB、pandas 打开） |
| 原始存储格式 | `kubectl -n spark-lab exec deploy/silo -- ls -laR /data/spark-data` | `xl.meta`、`part.N` |

**数据寿命：** `./stopcluster.sh`、退出 Docker Desktop、Mac 重启 → 保留；`./stopcluster.sh --purge`、Docker Desktop 的 Reset Kubernetes Cluster → 删除；删除用户 → 删除该用户的 `users/<用户名>/` 和 event log。

---

## 7. 组件、文件与版本

### 7.1 命名空间与 Pod

```text
traefik/traefik-*                    Ingress Controller，对外端口 8080
headlamp/headlamp-*                  Headlamp（仅管理员）
spark-lab/portal-*                   nginx：入口页面 + 按用户转发 Spark UI
spark-lab/hub-*                      JupyterHub：NativeAuthenticator、KubeSpawner、用户命名空间初始化
spark-lab/proxy-*                    configurable-http-proxy：/hub 和 /user/* 的路由
spark-lab/silo-*                     对象存储：S3 API 9000，Console 9001
spark-lab/spark-history-*            Spark History Server：18080
user-<用户名>/jupyter-<用户名>         用户 Notebook；Local / Client Mode 下也是 Spark Driver
user-<用户名>/*-exec-N                Spark Executor（只在作业运行期间存在）
user-<用户名>/*-driver                Cluster Mode Driver（作业结束后保持 Completed）
```

### 7.2 版本

| 组件 | 版本 | 说明 |
| :--- | :--- | :--- |
| Docker Desktop / Kubernetes | Engine 29.9.2 / v1.36.1 | kind 模式单节点，containerd 2.3.1，节点可分配 18 CPU / 16 GiB |
| Traefik | chart 42.4.1 / Proxy v3.7.19 | `traefik/values.yaml` |
| JupyterHub | chart 4.4.5 / JupyterHub 5.5.5 / KubeSpawner 7.1.0 / NativeAuthenticator 1.3.0 | `jupyterhub/values.yaml`、`jupyterhub/spark_lab_hook.py` |
| Headlamp | chart 0.46.0 / v0.46.0（arm64） | `headlamp/values.yaml` |
| JupyterLab | 4.4.7 | 运行时镜像中 |
| Apache Spark | 4.1.0（Scala 2.13.17） | 基础镜像 `apache/spark@sha256:370ada68…`（linux/arm64） |
| Java / Python | OpenJDK 21.0.9（Temurin）/ 3.10.12 | Notebook、Driver、Executor、History Server 使用同一个镜像 |
| Hadoop client / hadoop-aws / AWS SDK v2 bundle | 3.4.2 / 3.4.2 / 2.29.52 | S3A |
| Silo | `RELEASE.2026-11-12T00-00-00Z`（AGPLv3） | `pgsty/silo@sha256:c20d1123…`，PGSTY 维护的 MinIO 社区分支，不是 MinIO 官方产品 |
| Portal | nginx 1.29.8 | `nginxinc/nginx-unprivileged@sha256:0c79d56a…` |

### 7.3 文件索引

| 文件 | 作用 |
| :--- | :--- |
| `startcluster.sh` / `stopcluster.sh` | 启动 / 停止（§1） |
| `scripts/lab.env` | 共享配置：context、端口、镜像 tag、chart 版本 |
| `scripts/common.sh` | 脚本公共函数：等待集群就绪、构建输入哈希、把镜像导入节点 |
| `scripts/silo-export.sh` | 把 Silo 中的对象导出到 `data/silo-export/` |
| `scripts/headlamp-token.sh` | 生成 Headlamp 只读登录 token |
| `images/Dockerfile` | 统一运行时镜像（Spark 4.1.0、S3A JAR、JupyterLab、示例） |
| `images/spark-defaults.conf` | Spark 默认配置：UI 代理跳转、S3A 默认凭证、event log |
| `jupyterhub/values.yaml` | JupyterHub：认证、单用户镜像、init container、环境变量 |
| `jupyterhub/spark_lab_hook.py` | 用户命名空间初始化（`pre_spawn_hook`）和删除用户时的清理 |
| `traefik/values.yaml`、`headlamp/values.yaml` | Traefik、Headlamp 的 Helm values |
| `portal/index.html`、`portal/default.conf.template` | 入口页面；nginx 配置（按用户转发 Spark UI） |
| `k8s/00-namespace.yaml` | 平台命名空间 `spark-lab` |
| `k8s/10-spark-rbac.yaml` | ClusterRole `spark-runner`（通过用户命名空间中的 RoleBinding 生效） |
| `k8s/11-hub-rbac.yaml` | Hub 的集群级权限（创建和初始化用户命名空间） |
| `k8s/30-silo.yaml` | Silo Deployment、PVC、Service |
| `k8s/40-portal.yaml` | Portal Deployment 和 Service |
| `k8s/50-ingress.yaml` | 全部 Ingress 路由和 Traefik Middleware |
| `k8s/60-history-server.yaml` | Spark History Server |
| `k8s/70-headlamp-viewer.yaml` | Headlamp 登录用的只读 ServiceAccount |
| `k8s/templates/90-networkpolicy-optional.yaml` | 可选的 NetworkPolicy 模板（未启用；本环境的 kindnet 也不执行 NetworkPolicy） |
| `jobs/`、`notebooks/` | 示例，构建时复制进镜像 |
| `tests/` | 多用户验收脚本：`phase2_acceptance.sh`、`phase3_acceptance.sh`（`--cleanup` 结束时删除测试用户） |
| `docs/spark_usermanagement_design.md` | 多用户设计、安全分析、验收记录 |
| `.lab/credentials.env` | 本地凭证（自动生成，不提交到 Git） |

---

## 8. 排错与安全

### 8.1 常见问题

| 现象 | 原因 / 处理 |
| :--- | :--- |
| 注册后登录提示用户名或密码无效 | 还没被批准：管理员在 `/hub/authorize` 点击该用户名 |
| Spark UI 卡片显示"未运行 (502)"，或页面显示 "No Spark UI is running here" | 该用户没有存活的 SparkSession（或 Cluster Mode Driver 不在 Running），这是正常状态 |
| `http://jupyter-<用户名>:4040` 打不开 | 集群内地址，请用 `http://localhost:8080/spark-ui/<用户名>/` |
| Spark UI 先显示 "Spark is starting up" | Driver 刚启动，几秒后刷新 |
| `startcluster.sh` 提示 Notebook Pod 使用旧镜像 | JupyterLab 中 File → Hub Control Panel → Stop My Server → Start My Server |
| Executor 一直不出现 | 1) 超出命名空间配额：`kubectl -n user-<用户名> describe quota`；2) Pod 规格违反 Pod Security（如设置了 hostPath），Driver 日志中有 `violates PodSecurity "baseline:v1.36"`，Spark 会一直重试，需要 `spark.stop()` |
| `pods is forbidden ... in the namespace "user-xxx"` | 只能在自己的命名空间提交，使用 `--conf spark.kubernetes.namespace=$SPARK_LAB_NAMESPACE` |
| History Server 中没有刚结束的应用 | 等待约 10 秒；运行中的应用要结束后才有完整内容 |
| SparkSession 启动时报 event log 目录不存在 | 重新运行 `./startcluster.sh`，它会创建 `s3a://spark-data/spark-logs/` |
| 用户 Notebook 启动后 30 秒被删除 | 查看 Hub 日志 `kubectl -n spark-lab logs deploy/hub`；通常是单用户服务器无法连接 Hub |
| 局域网其他机器无法访问 | 确认 Mac 防火墙允许 Docker 接受入站连接，以及两台机器在同一网段 |

### 8.2 为什么镜像要"导入节点"

Docker Desktop 的 Kubernetes 运行在 kind 模式下，节点有自己的 containerd。`docker build` 产生的镜像在 Pod 中不可见，使用 `imagePullPolicy: Never` 时 Pod 会报 `ErrImageNeverPull`（实测）。`startcluster.sh` 临时创建一个特权 Pod `kube-system/spark-lab-image-loader`，进入节点的 mount namespace 执行 `ctr -n k8s.io images import`，完成后删除。镜像带有构建输入哈希标签 `org.sparklab.inputs-hash`，节点上已有相同哈希的镜像时跳过导入。

### 8.3 安全边界

已经做到的（均有验收证据，见 `docs/spark_usermanagement_design.md` §7）：

- 每个用户独立的用户名和密码，管理员批准后才能使用。
- 用户的 Kubernetes 身份 `user-<用户名>/spark-runner` 只能在自己的命名空间创建和删除 Pod，不能读 Secret，不能修改命名空间标签，不能为别人申请 token。
- Pod Security baseline 拒绝特权、hostPath、hostNetwork、hostPID、hostIPC、hostPort 和额外 capabilities，不管请求来自 Spark 参数还是直接调用 API。没有这一层，用户可以通过特权 Pod 拿到节点 root，进而拿到 cluster-admin。
- Notebook 和 Spark Pod 不持有 Silo root 凭证。

接受的风险（练手项目）：

- 没有 TLS，密码以明文在局域网传输；8080 端口监听在所有网卡上。只在可信局域网中使用，不要做公网端口映射。
- Spark UI 和 History Server 不鉴权；所有用户共享一个 Silo 凭证，能读写彼此的数据。
- Pod 之间网络互通（Docker Desktop 的 kindnet 不执行 NetworkPolicy，已实测）。
- 管理员 `spark` 可以进入任何用户的 Notebook。
- Hub 拥有集群范围的权限（创建用户命名空间所需），Hub 被攻破等于集群被攻破。
