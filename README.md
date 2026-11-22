# Spark Platform Learning Lab — Phase 1

个人 Apache Spark 交互式实验平台，运行在 Mac 上的 **Docker Desktop Kubernetes (arm64)**。浏览器登录 JupyterHub，在 Notebook 中运行 PySpark，分别观察 Local、Kubernetes Client、Kubernetes Cluster 三种模式下 Driver 与 Executor 的位置和生命周期，并通过 Hadoop S3A 读写 Silo 对象存储。

- 一条命令启动：`./startcluster.sh`
- 一个入口：`http://localhost:8080/`（其他机器用 `http://<Mac 的局域网 IP>:8080/`）
- 不需要任何 `kubectl port-forward`

设计背景与阶段计划见 [solution_design.md](solution_design.md)。

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
2. 构建统一运行时镜像 `spark-lab-runtime:0.3.0`，并导入 Kubernetes 节点（见 §7.2）。`images/`、`jobs/`、`notebooks/` 没有变化时跳过；
3. 第一次运行时生成本地凭证文件 `.lab/credentials.env`（已在 `.gitignore` 中）；
4. 部署 Namespace、RBAC、Driver Services、Silo，并创建 Bucket `spark-data`；
5. 发布入口页面 `portal/index.html`；
6. 安装 Traefik Ingress Controller，在 Mac 的 8080 端口对外提供服务；
7. 安装或升级 JupyterHub；
8. 等待 `http://localhost:8080/hub/health` 返回 200，然后打印访问地址和登录凭证。

实测耗时：Docker Desktop 已运行且没有改动时约 4 秒；Docker Desktop 未运行时约 1 分钟；修改 `jobs/` 或 `notebooks/` 后需要重建镜像，约 25 秒。

输出示例：

```text
Spark Platform Learning Lab is up.

  Portal (all links):  http://localhost:8080/
  From the LAN:        http://192.168.3.105:8080/

  JupyterHub login:    user=spark  password=<随机生成>
  Silo Console login:  accessKey=silo-admin  secretKey=<...>
  (stored in .lab/credentials.env)
```

想改密码：编辑 `.lab/credentials.env`，然后重新运行 `./startcluster.sh`。

### 1.3 停止

```bash
./stopcluster.sh            # 停止所有实验 Pod，保留全部数据
./stopcluster.sh --purge    # 卸载全部组件并删除所有数据（需要输入 spark-lab 确认）
```

| | `./stopcluster.sh` | `./stopcluster.sh --purge` |
| :--- | :--- | :--- |
| Spark Driver / Executor Pods | 删除 | 删除 |
| Notebook Pod（正在运行的 kernel 和未保存的修改会丢失） | 删除 | 删除 |
| Hub、Proxy、Silo、Portal、Traefik | 副本数缩到 0 | 卸载 |
| PVC：Notebook 主目录、Silo 对象、Hub 数据库 | **保留** | **删除** |
| Docker Desktop | 继续运行 | 继续运行 |

停止后再次运行 `./startcluster.sh` 即可恢复，数据都在。已验证：停止前在 Notebook 主目录写入的文件和 Silo 中的对象在重启后仍然存在。

> 停止期间 Docker Desktop 仍然占用 Mac 的 8080 端口（Traefik 的 LoadBalancer Service 还在），只是没有服务响应。如果需要释放这个端口，请使用 `--purge`，或者退出 Docker Desktop。

---

## 2. 统一入口（Portal）

打开 `http://localhost:8080/`。每个服务都有一张卡片，并显示该服务当前是否可达。

| 路径 | 后端 | 何时可用 |
| :--- | :--- | :--- |
| `/` | Portal（nginx，静态 `portal/index.html`） | 始终 |
| `/hub/`, `/user/spark/` | JupyterHub（`proxy-public`） | 始终 |
| `/spark-ui/` | Notebook Pod 4040 端口：Local 或 Client Mode 的 Driver | Notebook 中的 SparkSession 存活期间 |
| `/spark-driver/` | 正在运行的 Cluster Mode Driver Pod 的 4040 端口 | Driver Pod 处于 Running 期间 |
| `/silo/` | Silo Web Console | 始终 |

### 2.1 实现方式

```text
浏览器（本机或局域网）
   │  http://<host>:8080
   ▼
Docker Desktop 在 Mac 所有网卡上监听 *:8080   ← LoadBalancer Service（traefik 命名空间）
   ▼
Traefik v3.7（Ingress Controller）               ← k8s/50-ingress.yaml：4 个 Ingress + 5 个 Middleware
   ├── /               → Service portal:8080
   ├── /hub, /user     → Service proxy-public:80 → hub / jupyter-spark
   ├── /spark-ui       → Service spark-client-driver:4040 → jupyter-spark Pod
   ├── /spark-driver   → Service spark-cluster-driver-ui:4040 → 带 spark-role=driver 标签的 Pod
   └── /silo           → Service silo:9001
```

- **为什么选 Traefik：** 社区版 ingress-nginx 已于 2026 年 3 月退役，之后不再发布版本，也没有安全修复（[Kubernetes 官方迁移说明，由 Traefik 文档转述](https://doc.traefik.io/traefik/migrate/nginx-to-traefik/)）。Traefik 支持标准的 Kubernetes Ingress API，用一个 Helm release 即可安装。
- **为什么局域网可以访问：** Docker Desktop 会把 `type: LoadBalancer` 的 Service 发布在 Mac 的所有网卡上（实测监听地址为 `*:8080`），所以 `http://<Mac IP>:8080` 可以直接访问，不需要 port-forward。macOS 防火墙当前是关闭状态；如果你打开它，需要允许 Docker 接受入站连接。
- **Spark UI 子路径：** Traefik 先去掉 `/spark-ui` 前缀，再加上请求头 `X-Forwarded-Context: /spark-ui`。
  - 页面里的链接：Spark 默认就会根据这个请求头生成，例如 `/spark-ui/jobs/`。
  - Spark 自己发出的 302 跳转（例如 `/` → `/jobs/`、`/environment` → `/environment/`）：只有设置了 `spark.ui.proxyRedirectUri` 才会带上前缀。否则浏览器会跳到 `http://localhost:8080/jobs/`，得到 404。
  - 所以镜像内置了 `/opt/spark/conf/spark-defaults.conf`，内容为 `spark.ui.proxyRedirectUri /`（源文件 `images/spark-defaults.conf`）。设为 `/` 时，跳转保留浏览器使用的主机和端口（localhost 或局域网 IP 都可以），并加上前缀。实测 `/spark-ui/` → `/spark-ui/jobs/`，`/spark-driver/` → `/spark-driver/jobs/`。
- **Silo Console 子路径：** 设置环境变量 `MINIO_BROWSER_REDIRECT_URL=http://localhost:8080/silo`，Console 会从中取出路径 `/silo/`。Console 不检查主机名，因此通过局域网 IP 访问也能登录（已用两种地址验证登录 API 和 Bucket 列表）。

### 2.2 Spark UI 的限制

- **不要点 `http://jupyter-spark:4040`。** `sc.uiWebUrl`，以及 Notebook 中显示 `spark` 对象时出现的 "Spark UI" 链接，给出的都是集群内地址。浏览器解析不了 `jupyter-spark` 这个名字，请使用 `http://localhost:8080/spark-ui/`。示例 Notebook 会直接打印正确的地址。
- **Cluster Mode Driver 不读取镜像里的 spark-defaults.conf。** `spark-submit` 会把提交端的配置生成 ConfigMap，挂载到 Driver Pod 的 `/opt/spark/conf`，镜像中的同名文件被覆盖。提交端（Notebook Pod）的 defaults 会被带过去，所以从 Notebook 提交的作业不受影响。
- `/spark-ui/` 只转发 4040 端口。示例 Notebook 检测到自己不在 4040 时会提示。同一个 Notebook Pod 中再启动第二个 SparkSession（例如另一个 kernel）时，它会使用 4041 端口，这个端口不会被转发。
- 作业结束后 UI 随 Driver 一起消失。保留历史 UI 需要 Spark History Server，计划在 Phase 2 加入（见 solution_design.md §14）。
- 同时运行多个 Cluster Mode Driver 时，`/spark-driver/` 会在它们之间轮流转发。
- Spark UI 没有认证，局域网内任何人都能看到，并能使用 UI 上的 kill 按钮终止作业。

---

## 3. 登录 Notebook，运行示例

1. 打开 `http://localhost:8080/hub/`，用 `startcluster.sh` 打印的用户名和密码登录（用户名为 `spark`）。
2. JupyterHub（KubeSpawner）会创建 Pod `jupyter-spark`，进入 JupyterLab。
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

**示例目录每次启动 Notebook Server 时都会从镜像重新复制。** 要修改示例，请先把它复制到 `work/` 下的其他位置，否则下次启动时修改会被覆盖。

### 3.1 示例是如何进入 Notebook 的

**示例 Notebook 与 Silo 无关。** Notebook 文件（`.ipynb`）在 Notebook Pod 的 PVC 上，Silo 中只有 Spark 运行时写入的数据对象。

```text
jobs/ + notebooks/（本仓库）
   │  docker build（images/Dockerfile: COPY）
   ▼
镜像内 /opt/spark-lab/examples/
   │  Notebook Pod 的 init container "prepare-home"（jupyterhub/values.yaml）
   ▼
PVC claim-spark 中的 /home/jovyan/work/examples/   ← JupyterLab 中看到的就是这里
```

init container 以 root 身份运行，还负责一件事：把 `/home/jovyan` 的属主设为 UID 1000。local-path 存储类会忽略 `fsGroup`，新建的 PVC 默认属于 root。原 README 中需要手动执行的 `chown` 已经不需要了。

镜像内的 `/opt/spark-lab/examples/jobs/` 同样存在于 Cluster Mode 的 Driver Pod 中，因此 `spark-submit` 可以直接使用 `local:///opt/spark-lab/examples/jobs/wordcount.py`。

### 3.2 `01_wordcount_local.ipynb`：Local Mode

`master("local[*]")`：Driver JVM 由 Notebook kernel 通过 Py4J 启动，所有 task 都在这个 JVM 的线程中执行，不创建任何 Executor Pod。

实测输出（节选）：

```text
master       : local[*]
driver Pod   : jupyter-spark
[('kubernetes', 1), ('notebook', 1), ('pyspark', 1), ('spark', 3)]
host=jupyter-spark  python-worker-pid=144
host=jupyter-spark  python-worker-pid=145
JVM executors seen by the Driver: 1 (1 = the Driver itself)
```

Python worker 的 PID 与 kernel PID 不同：RDD lambda 运行在 Driver JVM 派生出的独立 Python worker 进程里，但这些进程都在同一个 Pod 中。

### 3.3 `02_wordcount_executor.ipynb`：Kubernetes Client Mode

Driver 是这个 Notebook 的 kernel，位于 Pod `jupyter-spark` 内。Driver 通过 Kubernetes API（ServiceAccount `spark-client`）创建 2 个 Executor Pod。Executor 通过 headless Service `spark-client-driver.spark-lab.svc:7078/7079` 回连 Driver。

运行时在 Mac 上另开一个终端观察：

```bash
kubectl -n spark-lab get pods -l spark-role=executor -w
```

浏览器中打开 `http://localhost:8080/spark-ui/executors/` 查看 Executors 页面。

实测输出（节选）：

```text
master     : k8s://https://kubernetes.default.svc:443
deployMode : client
driver Pod : jupyter-spark
registered executors: 2
tasks ran on: ['wordcount-k8s-client-95ae37a1ff064d04-exec-1', 'wordcount-k8s-client-95ae37a1ff064d04-exec-2']
executor id=driver  hostPort=spark-client-driver.spark-lab.svc:7079 completedTasks=0
executor id=     2  hostPort=10.244.0.35:7079             completedTasks=12
executor id=     1  hostPort=10.244.0.34:7079             completedTasks=12
driver python  : 3.10.12
executor python: 3.10.12
```

task 中读取的主机名就是 Executor Pod 的名字，Spark REST API 返回的 `hostPort` 就是对应 Executor Pod 的 IP。这样可以把 Spark UI 中的 Executor 与 Kubernetes Pod 一一对应起来。执行最后一个单元格 `spark.stop()` 后，Executor Pod 会被删除。

**同一时间只能有一个 Client Mode Driver。** 7078/7079 端口和 headless Service 都是固定的。在终端运行 `wordcount_executor.py` 之前，先停掉 Notebook 里的 Client Mode Session。

### 3.4 终端：Local 和 Client Mode 脚本

在 JupyterLab 的 Terminal 中运行：

```bash
python3 ~/work/examples/jobs/wordcount_local.py
python3 ~/work/examples/jobs/wordcount_executor.py
```

### 3.5 Cluster Mode（spark-submit）

在 JupyterLab 的 Terminal 中提交。Notebook Pod 只负责提交，Kubernetes 会创建独立的 Driver Pod：

```bash
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode cluster \
  --name wordcount-cluster \
  --conf spark.kubernetes.namespace=spark-lab \
  --conf spark.kubernetes.container.image=$SPARK_LAB_RUNTIME_IMAGE \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-client \
  --conf spark.kubernetes.driverEnv.WORDCOUNT_HOLD_SECONDS=60 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.kubernetes.executor.request.cores=500m \
  --conf spark.kubernetes.driver.request.cores=500m \
  local:///opt/spark-lab/examples/jobs/wordcount.py
```

- `local://` 指的是 **Driver Pod 镜像内** 的路径，不是 Notebook PVC 中的路径。脚本在镜像中，所以这样写可以运行。
- `WORDCOUNT_HOLD_SECONDS=60` 让 Driver 在作业完成后再保持 60 秒，这段时间可以打开 `http://localhost:8080/spark-driver/` 查看 UI。

实测 Pod 变化：

```text
wordcount-cluster-…-driver   Running
wordcount-kubernetes-cluster-…-exec-1/2   Running     ← Driver Pod 创建的 Executors
wordcount-cluster-…-driver   Completed               ← Executors 已删除，Driver Pod 保留，可查看日志
```

#### Driver Pod 是怎么被创建的

是的，Driver Pod 由 Notebook 中运行的 `spark-submit` 通过 Kubernetes API 创建，不需要 kubectl 或任何额外的控制器：

```text
jupyter-spark Pod
  ├─ spec.serviceAccountName: spark-client          ← jupyterhub/values.yaml: singleuser.serviceAccountName
  ├─ /var/run/secrets/kubernetes.io/serviceaccount/  ← Kubernetes 自动挂载该 SA 的 token、ca.crt、namespace
  └─ spark-submit --master k8s://https://kubernetes.default.svc:443 --deploy-mode cluster
       │  Spark 内置的 Kubernetes 客户端（fabric8）读取上面的 token
       │  POST /api/v1/namespaces/spark-lab/pods       → <name>-driver Pod
       │  POST .../services、.../configmaps            → Driver Service 和 spark.conf
       ▼
  API Server 用 RBAC 校验 spark-client 的权限          ← k8s/10-spark-rbac.yaml: Role spark-driver
       ▼
<name>-driver Pod（serviceAccountName 由 spark.kubernetes.authenticate.driver.serviceAccountName 指定，同样是 spark-client）
  └─ Driver 用自己挂载的 token，再 POST 创建 Executor Pods
```

`spark-client` 的实际权限（`kubectl auth can-i ... --as=system:serviceaccount:spark-lab:spark-client`）：

| 操作 | 结果 |
| :--- | :--- |
| 在 spark-lab 中 create / delete pods、create services | yes |
| get secrets | no |
| create deployments | no |
| 在其他命名空间 create pods | no |

Cluster Mode 的 Driver 能拿到 Silo 凭证，是因为 Pod spec 中写了 `secretKeyRef`，由 kubelet 在启动容器时注入，`spark-client` 本身并不需要读取 Secret 的权限。

查看 Driver 日志：`kubectl -n spark-lab logs <driver-pod>`。清理已完成的 Driver Pod：`kubectl -n spark-lab delete pod -l spark-role=driver`（`stopcluster.sh` 也会清理）。

**Cluster Mode + S3A：** Driver Pod 不继承 Notebook 的环境变量，凭证要从 Kubernetes Secret 注入，不要写在命令行上：

```bash
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 --deploy-mode cluster --name wordcount-s3a \
  --conf spark.kubernetes.namespace=spark-lab \
  --conf spark.kubernetes.container.image=$SPARK_LAB_RUNTIME_IMAGE \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-client \
  --conf spark.kubernetes.driverEnv.SILO_ENDPOINT=$SILO_ENDPOINT \
  --conf spark.kubernetes.driver.secretKeyRef.SILO_ACCESS_KEY=silo-root-credentials:accessKey \
  --conf spark.kubernetes.driver.secretKeyRef.SILO_SECRET_KEY=silo-root-credentials:secretKey \
  --conf spark.kubernetes.driverEnv.SILO_INPUT=s3a://spark-data/input/sample.txt \
  --conf spark.kubernetes.driverEnv.SILO_OUTPUT=s3a://spark-data/cluster-wordcount/output \
  --conf spark.executor.instances=2 --conf spark.executor.memory=1g --conf spark.executor.cores=1 \
  --conf spark.kubernetes.executor.request.cores=500m --conf spark.kubernetes.driver.request.cores=500m \
  local:///opt/spark-lab/examples/jobs/wordcount.py
```

已验证：作业状态为 `Succeeded`；结果文件 `cluster-wordcount/output/part-00000-*.json` 由 Executor 写入 Silo，内容为 `{"word":"spark","count":3}` 等。Driver 在 SparkSession 中设置的 `spark.hadoop.fs.s3a.*` 会随 Hadoop 配置传给 Executor，Executor 不需要另外注入凭证。

### 3.6 Local Mode 读写 Silo（S3A）

`01_wordcount_local.ipynb`、`02_wordcount_executor.ipynb` 和 `wordcount_local.py` **都不访问 Silo**；读写 Silo 请看 §3.7 的 `03_wordcount_s3a.ipynb`：输入来自内存中的 `parallelize([...])`，结果只用 `print` 输出。写入 Silo 的示例有两个：本节的 `test_s3a.py`，以及 §3.5 中设置了 `SILO_OUTPUT` 的 Cluster Mode 作业。

```bash
python3 ~/work/examples/jobs/test_s3a.py
```

这个脚本先写入文本，再用 S3A 读回并计算 WordCount，最后把结果写成 Parquet 并读回验证。每次运行都写到 `s3a://spark-data/run-<时间戳>/` 下。S3A 配置项（endpoint、path-style、凭证）请看脚本源码。

### 3.7 `03_wordcount_s3a.ipynb`：在 Notebook 中读写 Silo

一步步演示：配置 S3A → 写入输入 → 列出对象 → 读取并计算 WordCount → 写成 Parquet → 列出输出 → 读回 → 在 Silo Console 中查看。

- 数据路径：`s3a://spark-data/notebook-demo/<用户名>/input/` 和 `.../output/wordcount.parquet/`。
- 第一个代码单元格 `EXECUTORS = "local"`（默认）：所有读写都在 Notebook Pod 中完成。
- 改为 `"k8s"`：Client Mode，由 Executor Pod 读写 Silo。运行前先停掉 02 的 Session。
- 凭证来自 Pod 环境变量（Secret `silo-root-credentials`），不会出现在 Notebook 中。

两种模式都已实测。`k8s` 模式的输出（节选）：

```text
read 2 line(s) from Silo on wordcount-s3a-k8s-dbbb4aa2028c2941-exec-2
read 2 line(s) from Silo on wordcount-s3a-k8s-dbbb4aa2028c2941-exec-1
s3a://spark-data/notebook-demo/spark/output/wordcount.parquet
  file      0 B  _SUCCESS
  file    920 B  part-00000-21b8fdb5-…-c000.snappy.parquet
```

Silo Console 的 API 列出的是同样两个对象（`_SUCCESS` 0 B，`part-00000-….snappy.parquet` 920 B）。在浏览器中打开 `http://localhost:8080/silo/` → `spark-data` → `notebook-demo/spark/` 即可看到。

---

## 4. Silo 对象存储

### 4.1 Web 界面

有。Silo 内置完整的 Web Console（Silo `RELEASE.2026-11-12T00-00-00Z` 内嵌 Console v2.7.2）。

- 地址：`http://localhost:8080/silo/`
- 登录：`startcluster.sh` 打印的 accessKey / secretKey（也在 `.lab/credentials.env` 中）
- 功能：浏览 Bucket 和目录，预览、下载、上传、删除对象，查看对象元数据

S3 API 端点只在集群内提供：`http://silo.spark-lab.svc:9000`，没有经过 Ingress 暴露。Spark 都在集群内运行，不需要从外部访问 S3 API。

### 4.2 存储是怎么挂载的

> 仓库中的 `data/` 目录**没有**挂载给 Silo。`data/silo-export/` 只是 `scripts/silo-export.sh` 的输出目录，里面是从 Silo 下载回来的副本（已加入 git-ignore）。删除或修改它不会影响 Silo 中的数据。

```text
Silo Pod 中的 /data
   │  volumeMount
   ▼
PVC silo-data（5Gi，StorageClass standard，provisioner rancher.io/local-path）
   │  local-path 在节点上创建一个目录
   ▼
Kubernetes 节点 desktop-control-plane 上的目录
/var/local-path-provisioner/pvc-<uid>_spark-lab_silo-data/
   │  节点本身是 Docker Desktop Linux 虚拟机里的一个容器（kind 模式）
   ▼
虚拟机磁盘镜像 ~/Library/Containers/com.docker.docker/Data/vms/0/data/Docker.raw
   │
   ▼
Mac 本地硬盘
```

Notebook 的主目录（PVC `claim-spark`，挂载到 `/home/jovyan`）和 Hub 数据库（PVC `hub-db-dir`）使用同样的链路。

### 4.3 能否在 Mac 本地硬盘上看到实际的存储文件？

**不能直接看到。** 数据确实保存在 Mac 硬盘上，但有两层阻隔：

1. **数据在虚拟机磁盘镜像里。** 所有字节都在一个大文件 `Docker.raw` 中，Finder 无法打开。Docker Desktop 的 kind 节点没有挂载 Mac 的 `/Users` 目录：实测把 Mac 目录用 `hostPath` 挂进 Pod 时报 `not a directory`。
2. **Silo 使用 MinIO 的 xl 存储格式，不是普通文件。** 每个对象是一个目录，里面是 `xl.meta` 元数据文件：
   - 小对象的数据直接内嵌在 `xl.meta` 中。例如 `spark-data/input/sample.txt/` 目录下只有一个 461 字节的 `xl.meta`。
   - 较大的对象另有数据目录。实测 2,000,000 字节的对象保存为 `<object>/xl.meta` 加 `<uuid>/part.1`，后者 2,000,064 字节，多出来的部分是完整性校验数据。

   所以即使把磁盘挂载到 Mac 上，看到的也是 `xl.meta`，不是 `.parquet` 文件。

**可以用这三种方式查看：**

| 方式 | 命令 / 地址 | 看到的内容 |
| :--- | :--- | :--- |
| Web Console | `http://localhost:8080/silo/` | 逻辑对象，可预览和下载 |
| 导出到 Mac | `scripts/silo-export.sh [bucket] [prefix]` | 普通文件，写到 `./data/silo-export/<bucket>/...` |
| 原始存储格式 | `kubectl -n spark-lab exec deploy/silo -- ls -laR /data/spark-data` | `xl.meta`、`part.N` |

导出示例（已验证：导出的 `.parquet` 文件首尾都是 `PAR1` 魔数，可以直接用 DuckDB、pandas 等工具打开）：

```bash
scripts/silo-export.sh                                   # 整个 spark-data bucket
scripts/silo-export.sh spark-data cluster-wordcount/     # 只导出某个前缀
find data/silo-export -type f
```

导出脚本使用 Silo 镜像自带的 `mcli` 客户端，Mac 上不需要安装任何 S3 工具。`data/silo-export/` 已加入 `.gitignore`。

**数据寿命：**

| 操作 | 结果 |
| :--- | :--- |
| `./stopcluster.sh`、退出 Docker Desktop、Mac 重启 | 数据保留 |
| `./stopcluster.sh --purge` | 数据删除 |
| Docker Desktop → Reset Kubernetes Cluster / Clean up data | 数据删除 |

重要数据请先用 `scripts/silo-export.sh` 导出。

---

## 5. 组件与版本

所有实验资源都在 `spark-lab` 命名空间中，Traefik 在 `traefik` 命名空间中。

运行时的 Pod：

```text
traefik/traefik-*          Ingress Controller，对外端口 8080
spark-lab/portal-*         nginx，提供入口页面
spark-lab/hub-*            JupyterHub：认证（DummyAuthenticator）和 KubeSpawner
spark-lab/proxy-*          configurable-http-proxy：/hub 和 /user/* 的路由
spark-lab/silo-*           对象存储：S3 API 9000，Console 9001
spark-lab/jupyter-spark    用户 Notebook；Local 和 Client Mode 下也是 Spark Driver
spark-lab/*-exec-N         Spark Executors（只在作业运行期间存在）
spark-lab/*-driver         Cluster Mode Driver（作业结束后保持 Completed 状态）
```

相比之前的部署，`user-scheduler`（2 个副本）、`continuous-image-puller` 和 `user-placeholder` 已经关闭。它们用于多节点集群的装箱调度和镜像预拉取，在单节点且镜像在本地构建的环境里没有作用。

| 组件 | 版本 | 说明 |
| :--- | :--- | :--- |
| Docker Desktop / Kubernetes | Engine 29.9.2 / v1.36.1 | kind 模式单节点，containerd 2.3.1，节点可分配 18 CPU / 16 GiB |
| Traefik | chart 42.4.1 / Proxy v3.7.19 | `traefik/values.yaml` |
| JupyterHub | chart 4.4.5 / JupyterHub 5.5.5 / KubeSpawner 7.1.0 | `jupyterhub/values.yaml` |
| JupyterLab | 4.4.7 | 运行时镜像中 |
| Apache Spark | 4.1.0（Scala 2.13.17） | 基础镜像 `apache/spark@sha256:370ada68…`（linux/arm64） |
| Java | OpenJDK 21.0.9（Temurin） | |
| Python | 3.10.12 | Notebook、Driver、Executor 使用同一个镜像 |
| Hadoop client / hadoop-aws | 3.4.2 / 3.4.2 | S3A |
| AWS SDK v2 bundle | 2.29.52 | Hadoop 3.4.2 锁定的版本 |
| Silo | `RELEASE.2026-11-12T00-00-00Z`（AGPLv3） | `pgsty/silo@sha256:c20d1123…`，PGSTY 维护的 MinIO 社区分支，不是 MinIO 官方产品 |
| Portal | nginx 1.29.8 | `nginxinc/nginx-unprivileged@sha256:0c79d56a…` |

---

## 6. 文件索引

| 文件 | 作用 |
| :--- | :--- |
| `startcluster.sh` / `stopcluster.sh` | 启动 / 停止（见 §1） |
| `scripts/lab.env` | 共享配置：context、端口、镜像 tag、chart 版本 |
| `scripts/common.sh` | 脚本公共函数：等待集群就绪、计算构建输入哈希、把镜像导入节点 |
| `scripts/silo-export.sh` | 把 Silo 中的对象导出到 `data/silo-export/` |
| `images/Dockerfile` | 统一运行时镜像（Spark 4.1.0、S3A JAR、JupyterLab、示例） |
| `images/spark-defaults.conf` | 镜像中的 Spark 默认配置（`spark.ui.proxyRedirectUri`） |
| `jupyterhub/values.yaml` | JupyterHub：单用户镜像、init container、PVC、环境变量 |
| `traefik/values.yaml` | Traefik：LoadBalancer 8080、IngressClass `traefik` |
| `portal/index.html` | 入口页面（发布为 ConfigMap `portal-html`） |
| `k8s/00-namespace.yaml` | 命名空间 |
| `k8s/10-spark-rbac.yaml` | `spark-client` ServiceAccount 和 Driver 所需的 Role |
| `k8s/20-client-driver-service.yaml` | Client Mode Driver 的 headless Service（7078/7079/4040） |
| `k8s/21-cluster-driver-ui-service.yaml` | Cluster Mode Driver UI 的 Service（selector `spark-role=driver`） |
| `k8s/30-silo.yaml` | Silo Deployment、PVC、Service |
| `k8s/40-portal.yaml` | Portal Deployment 和 Service |
| `k8s/50-ingress.yaml` | 全部 Ingress 路由和 Traefik Middleware |
| `k8s/templates/90-networkpolicy-optional.yaml` | 可选的 NetworkPolicy 模板（未启用） |
| `jobs/`, `notebooks/` | 示例，构建时复制进镜像 |
| `.lab/credentials.env` | 本地凭证（自动生成，不提交到 Git） |

---

## 7. 排错

### 7.1 常见问题

| 现象 | 原因 / 处理 |
| :--- | :--- |
| 点 `/spark-ui/` 后跳到 `http://localhost:8080/jobs/` 并显示 404 | Notebook Pod 使用的是修复前的镜像：Stop My Server → Start My Server |
| `http://jupyter-spark:4040` 打不开 | 这是集群内地址，请使用 `http://localhost:8080/spark-ui/`（§2.2） |
| Portal 上 Spark UI 卡片显示"未运行 (502)" | 没有存活的 SparkSession，这是正常状态 |
| `/spark-driver/` 返回 503 | 当前没有处于 Running 状态的 Cluster Mode Driver Pod |
| `startcluster.sh` 提示 Notebook Pod 使用旧镜像 | 在 JupyterLab 中选择 File → Hub Control Panel → Stop My Server，再点 Start My Server |
| Client Mode 卡住或报端口占用 | 另一个 Client Mode SparkSession 正在占用 7078/7079 端口，先 `spark.stop()` |
| `spark.stop()` 时报 `cannot deletecollection` | RBAC 是旧版本，重新运行 `./startcluster.sh`。Spark 4.1 在 stop 时会按标签批量删除 Service、ConfigMap 和 PVC |
| 局域网其他机器无法访问 | 确认 Mac 防火墙允许 Docker 接受入站连接，以及两台机器在同一网段 |

### 7.2 为什么镜像要"导入节点"

Docker Desktop 的 Kubernetes 运行在 kind 模式下，节点有自己的 containerd。`docker build` 产生的镜像在 Pod 中不可见，使用 `imagePullPolicy: Never` 时 Pod 会报 `ErrImageNeverPull`（实测）。

`startcluster.sh` 的处理方式：临时创建一个特权 Pod `kube-system/spark-lab-image-loader`，进入节点的 mount namespace，执行 `ctr -n k8s.io images import`，完成后删除这个 Pod。镜像上带有构建输入哈希标签 `org.sparklab.inputs-hash`，节点上已有相同哈希的镜像时跳过导入。

### 7.3 安全边界

这是个人学习环境：使用 DummyAuthenticator、没有 TLS、Spark UI 没有认证，而且 8080 端口监听在所有网卡上。只在可信局域网中使用，不要做公网端口映射。Notebook 使用的 `spark-client` ServiceAccount 可以在 `spark-lab` 命名空间内创建和删除 Pod。
