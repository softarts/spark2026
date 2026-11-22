先回答第 1 点：**Spark Local Mode 不只是安装一个 Python 库，而是需要一套本地 Spark Runtime 环境。** 如果使用 PySpark，通常需要 Python、Java、PySpark（包含 Spark 运行时组件），并确保版本兼容。

这个问题与你在 macOS 或 Windows 上运行 Spark Local Mode 完全相同。Notebook Pod 只是把这套运行环境放进了容器里，并没有改变 Spark 的运行原理。

下面我把两部分整理成一份技术说明：先讲本地运行环境，再统一梳理 Local、Client、Cluster 三种模式，以及它们在 JupyterHub / Kubernetes 平台中的职责和权限区别。

# **Spark 运行模式与 Runtime 环境设计说明**

适用范围：Spark + Kubernetes + JupyterHub 平台

## **第一部分：Spark Local Mode 需要什么 Runtime？**

### **1.1 Spark Local Mode 的运行原理**

Spark 是基于 JVM 的计算引擎。即使使用 Python 编写 PySpark 程序，底层仍然需要 Java Virtual Machine（JVM）运行 Spark Driver 和计算引擎。

所以，PySpark 并不是一个完全由 Python 实现的独立计算框架。

**Local Mode Runtime 架构**

**Python Application / Jupyter Notebook**

Python 代码、PySpark API

**Py4J Bridge**

Python 与 JVM Spark 进程之间的通信

**Spark Driver（JVM）**

SparkSession、SparkContext、任务调度

Local Worker 1

Local Worker 2

Local Worker 3

本地工作线程执行计算，不创建 Kubernetes Executor Pods

这里的 Worker 是本地计算线程，不是 Kubernetes Worker Node，也不是独立的 Executor Pod。

### **1.2 macOS / Windows 需要安装什么？**

如果使用 PySpark，基本运行环境如下：

| 组件                    | 是否必须 | 作用                                |
| --------------------- | ---- | --------------------------------- |
| Python                | 是    | 编写和运行 PySpark 程序                  |
| Java Runtime（JVM）     | 是    | 运行 Spark 引擎                       |
| PySpark               | 是    | Python API 和 Spark Runtime 组件     |
| JupyterLab / Notebook | 可选   | 交互式开发环境                           |
| Hadoop Client         | 视情况  | 访问 HDFS 等 Hadoop 服务               |
| Spark 独立安装包           | 可选   | 直接使用 spark-submit、spark-shell 等命令 |

Apache Spark 官方目前的 4.3.0 文档列出的环境要求为 Java 17、21 或 25，以及 Python 3.11 及以上；支持 macOS、Windows 和 Linux。

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Spark 4.3.0 Documentation+1



**一个重要区别：安装 PySpark 和安装完整 Spark 独立发行包，不是完全相同的事情。**

- `pip install pyspark`：适合 Python 用户本地开发，PyPI 包包含 Spark 运行组件。
- 下载 Spark 官方发行包：可以使用完整的 Spark 命令行工具，例如 `spark-submit`、`spark-shell`、`pyspark`。

如果只是要在 Notebook 里运行 Local Mode，通常安装 PySpark 就够了。

### **1.3 macOS 安装示例**

下面以 Spark 4.3.0 为例。

先安装 Java 17：

```
brew install openjdk@17
```

配置 Java 环境（具体路径根据 Homebrew 安装位置调整）：

```
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

检查：

```
java -version
```

创建 Python 环境：

```
python3 -m venv .venv
source .venv/bin/activate
```

安装 PySpark：

```
pip install pyspark==4.3.0
```

测试：

```
from pyspark.sql import SparkSessionspark = (    SparkSession.builder    .appName("local-test")    .master("local[*]")    .getOrCreate())spark.range(1000000).count()
```

如果返回：

```
1000000
```

说明基本 Local Mode 运行成功。

### **1.4 Windows 安装示例**

Windows 的原理完全相同。

1. 安装支持的 Java 版本，例如 Java 17。
2. 设置 `JAVA_HOME`。
3. 将 Java 的 `bin` 目录加入 `PATH`。
4. 安装 Python。
5. 创建虚拟环境并安装 PySpark。

PowerShell 示例：

```
py -3 -m venv .venv

.\.venv\Scripts\Activate.ps1

pip install pyspark==4.3.0
```

检查 Java：

```
java -version
```

运行 PySpark：

```
from pyspark.sql import SparkSessionspark = (    SparkSession.builder    .appName("windows-local-test")    .master("local[4]")    .getOrCreate())spark.range(1000000).show(10)
```

**关于 Windows 的 Hadoop 依赖：** 对普通 Local Mode 的 DataFrame、SQL、本地文件计算，不应把 `winutils.exe` 当作所有场景都必须安装的前置条件。但如果使用特定 Hadoop 文件系统、Hadoop 原生库或依赖 Windows 特有文件权限操作的功能，可能需要额外配置。不能将简单本地计算和 HDFS 环境混为一谈。

### **1.5 Runtime 版本如何选择？**

这一点对你的项目尤其重要。

不应该因为最新 Spark 是 4.3.0，就直接把所有环境升级到 4.3.0。

应该先确定项目 Spark 版本，然后统一：

| 环境                     | Spark 版本 | Java 版本       | PySpark 版本 |
| ---------------------- | -------- | ------------- | ---------- |
| macOS 开发机              | 项目指定版本   | 与该 Spark 版本兼容 | 对应版本       |
| Windows 开发机            | 项目指定版本   | 与该 Spark 版本兼容 | 对应版本       |
| Jupyter Notebook Image | 项目指定版本   | 与该 Spark 版本兼容 | 对应版本       |
| Spark Driver Image     | 项目指定版本   | 与该 Spark 版本兼容 | 对应版本       |
| Spark Executor Image   | 项目指定版本   | 与该 Spark 版本兼容 | 对应版本       |

例如，如果项目使用 Spark 3.5.x，就应该按 Spark 3.5.x 的兼容要求配置 Java 和 PySpark，而不是盲目使用 Spark 4.3.0 的环境要求。

官方安装说明：[PySpark Installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html?utm_source=chatgpt.com)。

## **第二部分：Spark 的三种运行模式**

这里要先建立一个核心概念：

**Spark 的运行模式，主要区别是 Driver 在哪里运行，以及 Executor 在哪里运行。**

它们并不意味着一定有 Spark Master。

### **2.1 Local Mode**

快速验证

[Understanding Apache Spark Memory Management in local mode: A Deep Dive with PySpark | by Pradosh Kumar Jena | DataUniverse | Medium](https://images.openai.com/static-rsc-4/52VJRDCBLRKdp7lJppdHVRd86bekZmTuI2VzvKo8_1DlgRCZbpNsYhU52Y_ilc6lOCdFhKV3OMlwXb3Z2B8HiODuiH8R3ZOqRDutOhlMkdQepPfYNY7lrKogaIMZ_2haUchJpSKVb4uO-uys053nNaM_y5vwYgbz3YH517WmGDI?purpose=inline)

[medium.com](https://medium.com/datauniverse/understanding-apache-spark-memory-management-in-local-mode-a-deep-dive-with-pyspark-70649b3ddde0?utm_source=chatgpt.com)

结构：

```
Notebook Pod / Local Machine
│
├── Python Application
│
├── PySpark
│
└── Spark Driver (JVM)
    │
    ├── Local Worker Thread 1
    ├── Local Worker Thread 2
    ├── Local Worker Thread 3
    └── Local Worker Thread N
```

特点：

- Driver 在 Notebook Pod 或本地机器运行。
- 计算任务在本地线程执行。
- 不创建独立 Executor Pod。
- 不需要 Kubernetes API 权限。
- 不需要 Spark Master。
- CPU 和内存都受 Notebook Pod 的资源限制。

常见配置：

```
.master("local[*]")
```

也可以：

```
.master("local[2]")
```

适用场景：

- 验证 PySpark 代码。
- 测试 Spark SQL。
- 验证数据处理逻辑。
- 小规模数据处理。
- 开发和调试。

需要注意：`local[*]` 表示使用本地可用的逻辑 CPU 线程，并不意味着可以使用 Kubernetes 集群全部 CPU。

### **2.2 Client Mode**

交互式分布式计算

[Running Apache Spark and Jupiter Notebooks on Kubernetes with Helm Charts | \[“Khalid Mammadov”\]](https://images.openai.com/static-rsc-4/eAGSmVrhQdTwfhD0FUwN539-MM-jcfc7xAytz3LX_BCYRUjrehBuDZatu9ddSHo9BOjMlNi41DMvBn6pTuhAQWt9tF_OrznJrzCgrb0klqbCXrniuf1Iw8sM2pQony3yPPk40Ow3m-ZbyY7ccLTkKPnMJmeRlnEk-zyLyuEosP8?purpose=inline)

[khalidmammadov.github.io](https://khalidmammadov.github.io/spark/spark_k8s_static_with_jupiter.html?utm_source=chatgpt.com)

结构：

```
Kubernetes
│
├── Notebook Pod (Alice)
│   │
│   └── Spark Driver (JVM)
│
├── Executor Pod 1
├── Executor Pod 2
└── Executor Pod 3
```

特点：

- Driver 在 Notebook Pod 内。
- Executor 是独立 Kubernetes Pods。
- Driver 通过 Kubernetes API 创建 Executor。
- Executor 需要能够通过网络连接 Driver。
- Notebook Pod 关闭后，Driver 也会退出。

这种模式下，Notebook 不再只是开发环境，它还承担 Spark Driver 的角色。

因此 Notebook 需要 Kubernetes API 凭据和创建 Executor 的权限。

Spark 官方文档特别强调 Client Mode 的网络连接：Executor 必须能够通过可路由的地址和端口访问 Driver。对于运行在 Pod 内的 Driver，通常需要考虑稳定的 Service 地址和 Driver 网络配置。

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Spark 4.2.0 Documentation+1



适用场景：

- 交互式分布式计算。
- Notebook 中调试较大的数据集。
- 希望 Notebook 保持 Driver 生命周期的任务。

### **2.3 Cluster Mode**

独立任务运行

[Deploy Spark on Kubernetes cluster | by Manik Malhotra | Analytics Vidhya | Medium](https://images.openai.com/static-rsc-4/QXwKbGtPodikF8pXVLQ22RrecC-0DixFSsb1pIux0tLxIiLjuc3tMvcOtROFPrC8UGes7FwiyisXffR7rpnPGZVSBns9tB0h8IMKY4zbv3iw2-Ybg0ytJiXlnZOddJQg_8gMhDXKFDwsKOHhJF-qGN7R6ivIrhaJLb9gkwLGTX0?purpose=inline)

[medium.com](https://medium.com/analytics-vidhya/deploy-spark-on-kubernetes-cluster-7779c658bf6c?utm_source=chatgpt.com)

结构：

```
Kubernetes
│
├── Notebook Pod (Alice)
│   │
│   └── spark-submit
│
├── Spark Driver Pod
│   │
│   └── Spark Application
│
├── Executor Pod 1
├── Executor Pod 2
└── Executor Pod 3
```

特点：

- Notebook 负责提交任务。
- Spark Driver 在独立 Pod 中运行。
- Driver 负责解析 Spark 程序、生成执行计划、划分 Stage 和 Task。
- Driver 通过 Kubernetes API 创建 Executor。
- Kubernetes Scheduler 负责决定 Driver 和 Executor Pods 运行在哪个 Node。
- Notebook 可以退出，而已经提交的 Spark 应用仍然可以继续运行。

这里最重要的概念是：

**Spark Driver 是 Spark 应用的调度中心，Kubernetes 是容器资源调度中心。**

二者负责不同层次的调度。

## **第三部分：三种模式的对比**

### **3.1 架构和生命周期**

| 比较项                 | Local Mode           | Client Mode  | Cluster Mode  |
| ------------------- | -------------------- | ------------ | ------------- |
| Driver 位置           | Notebook Pod         | Notebook Pod | 独立 Driver Pod |
| Executor 位置         | Driver 所在 JVM 内的本地线程 | 独立 Pod       | 独立 Pod        |
| 是否使用 Kubernetes API | 否                    | 是            | 是             |
| 是否创建 Executor Pod   | 否                    | 是            | 是             |
| 是否需要 Driver SA      | 否                    | Notebook SA  | Driver SA     |
| 是否需要 Spark Master   | 否                    | 否            | 否             |
| Notebook 退出后任务能否继续  | 不能                   | 不能           | 可以            |
| 适合场景                | 快速验证                 | 交互式分布式计算     | 独立任务运行        |

### **3.2 谁负责调度？**

这是理解 Spark 和 Kubernetes 关系最重要的一部分。

| 组件                    | 职责                   |
| --------------------- | -------------------- |
| Spark Driver          | 管理 Spark Application |
| Spark DAGScheduler    | 将 Job 划分成 Stage      |
| Spark TaskScheduler   | 将 Task 分配给 Executor  |
| Spark Executor        | 执行 Task              |
| Kubernetes API Server | 接收 Pod 创建等请求         |
| Kubernetes Scheduler  | 选择 Pod 运行的 Node      |
| Kubernetes Kubelet    | 在 Node 上运行容器         |

例如，一个 Spark 作业执行：

```
df = spark.read.parquet("s3a://bucket/orders")result = df.groupBy("region").sum("amount")result.show()
```

Spark Driver 理解这些计算逻辑，生成执行计划并安排 Task。

Kubernetes 并不理解 `groupBy`、`sum`、Stage 或 Task，它只负责 Pod、CPU、内存和节点调度。

### **3.3 Spark Master 是否需要？**

不需要。

这里容易混淆两个概念：

| 架构                  | 资源管理者                    | 是否有 Spark Master |
| ------------------- | ------------------------ | ---------------- |
| Spark Standalone    | Spark Master / Worker    | 有                |
| Spark on YARN       | YARN ResourceManager     | 没有               |
| Spark on Kubernetes | Kubernetes Control Plane | 没有               |

你项目中的：

```
--master k8s://https://kubernetes.default.svc
```

这里的 `master` 指 Kubernetes API Server 的连接地址，不是 Spark Standalone Master。

## **第四部分：JupyterHub 中的用户与权限设计**

结合你的平台，我建议采用三种运行模式，但使用不同的权限策略。

### **4.1 各组件的身份**

| 组件                   | ServiceAccount        | 是否需要 Kubernetes API 权限 |
| -------------------- | --------------------- | ---------------------- |
| JupyterHub Hub       | `hub`                 | 需要，管理 Notebook Pods    |
| Local Mode Notebook  | 普通 Notebook SA        | 不需要                    |
| Client Mode Notebook | `spark-client-driver` | 需要，创建 Executor Pods    |
| Cluster Mode Driver  | `spark-driver`        | 需要，创建 Executor Pods    |
| Spark Executor       | `spark-executor`      | 通常不需要                  |

### **4.2 Local Mode**

```
JupyterHub Hub
    │
    └── Notebook Pod (Alice)
          │
          └── Spark Driver
                │
                └── Local Worker Threads
```

权限：

- Notebook 不需要 Kubernetes API Token。
- 不需要 Spark Driver Role。
- 不需要 Executor ServiceAccount。
- 不需要创建额外的 Spark Pods。

这是最简单、最安全的 Spark 使用方式。

### **4.3 Client Mode**

```
JupyterHub Hub
    │
    └── Notebook Pod (Alice)
          │
          ├── ServiceAccount: spark-client-driver
          │
          └── Spark Driver
                │
                ├── Executor Pod 1
                ├── Executor Pod 2
                └── Executor Pod 3
```

权限：

- Notebook Pod 挂载 `spark-client-driver` ServiceAccount Token。
- 该身份获得限定 Namespace 内的 Pod 管理权限。
- Executor 使用独立的 `spark-executor` ServiceAccount。
- Executor 不需要创建其他 Pod 的权限。

这里要特别注意：Notebook 中运行的用户代码能够访问这个 ServiceAccount 的凭据。因此 Client Mode 不是单纯的 Spark 功能开关，它也改变了 Notebook 用户的 Kubernetes 权限。

### **4.4 Cluster Mode**

```
JupyterHub Hub
    │
    └── Notebook Pod (Alice)
          │
          └── spark-submit
                │
                ▼
          Spark Driver Pod
          ServiceAccount: spark-driver
                │
                ├── Executor Pod 1
                ├── Executor Pod 2
                └── Executor Pod 3
```

权限：

- Notebook 负责提交任务。
- Driver Pod 使用 `spark-driver` ServiceAccount。
- Driver 在自己的 Namespace 中创建 Executor。
- Executor 使用 `spark-executor` ServiceAccount。

Notebook 不需要获得完整的 Spark Driver 权限，但需要有安全的任务提交入口和相应的认证凭据。

### **4.5 每个用户是否需要一个 Role？**

不一定。

这里要区分：

- ServiceAccount：代表 Pod 的身份。
- Role：定义可以执行哪些 Kubernetes 操作。
- RoleBinding：把 Role 绑定给 ServiceAccount 或 User。

多个用户可以使用相同权限模板，但绑定到各自的 Namespace。

我的建议是：

**初期：** 继续使用项目现有 `spark-lab` Namespace，先区分 Hub、Notebook、Driver、Executor 身份。

**后期：** 每个用户一个 Namespace，例如：

```
spark-alice
spark-bob
spark-charlie
```

每个 Namespace 使用相同的 Role 模板，但是有自己的 ServiceAccount 和 RoleBinding。

这样才能形成更可靠的用户间权限隔离。单纯在同一个 Namespace 里给每个人一个 ServiceAccount，不能保证用户只能创建或访问自己名下的 Pod。

Spark 官方文档也说明，Driver 通常只需要在其应用所在 Namespace 内拥有相应权限，使用 Namespace 范围的 Role 即可，不必一律授予 ClusterRole。

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Spark 4.3.0 Documentation



## **第五部分：我建议的平台产品设计**

我建议不要把这三种模式隐藏在复杂的 Spark 配置中，而是把它们作为用户可选择的运行方式。

# **Spark Runtime Profiles**

建议的 JupyterHub / Spark 平台运行配置

**Profile 1：Local**

默认

# **0**

额外 Spark Pod 数量

- Notebook 内运行 Spark Driver。
- 使用本地计算线程。
- 无须 Kubernetes API 权限。
- 适合代码开发、快速验证。

**Profile 2：Client**

- Notebook 内运行 Spark Driver。
- Executor 在 Kubernetes 中运行。
- Notebook 需要 Driver 权限。
- 适合交互式分布式计算。

**Profile 3：Cluster**

- Notebook 只负责提交任务。
- Driver 和 Executor 都在 Kubernetes 中运行。
- Driver 使用独立 ServiceAccount。
- 适合正式批处理任务。

### **5.1 Notebook Image 如何设计？**

我建议不要为 Local Mode 单独维护一套完全不同的 Notebook Image，而是统一基础 Runtime。

例如：

```
spark-notebook:3.5.x
│
├── Python
├── Java
├── PySpark 3.5.x
├── JupyterLab
├── Spark SQL
├── Hadoop / Object Storage Connectors
└── Project Python Libraries
```

这样同一 Notebook Image 可以支持：

- Local Mode：直接在本地 JVM 中执行。
- Client Mode：Driver 在 Notebook Pod 中运行，Executor 通过 Kubernetes 创建。
- Cluster Mode：通过 `spark-submit` 提交应用。

但要注意，Cluster Mode 的 Driver / Executor Image 也必须具有匹配的 Spark Runtime 和应用依赖。Notebook Image 与 Spark Image 可以复用基础层，但不要求完全相同。

### **5.2 用户资源统计**

你之前关心每个用户用了多少 Pod、Spark 任务进度等问题。三种模式的统计口径应该不同：

| 统计项               | Local           | Client                   | Cluster                |
| ----------------- | --------------- | ------------------------ | ---------------------- |
| Notebook Pod      | 1               | 1                        | 1                      |
| Spark Driver Pod  | 0               | 0                        | 1                      |
| Executor Pod      | 0               | N                        | N                      |
| Spark Application | 1 或多个           | 1 或多个                    | 1 或多个                  |
| CPU / Memory      | 计入 Notebook Pod | Notebook + Executor Pods | Driver + Executor Pods |

这里的 Local Mode 可能同时运行多个 Spark Application，但它们共用 Notebook Pod 内的资源，因此不能把每个 Spark Application 都算成独立 Pod。

如果要统计每个用户的 CPU 和内存：

- Local：统计 Notebook Pod。
- Client：统计 Notebook Pod 和对应 Executor Pods。
- Cluster：统计 Driver Pod 和 Executor Pods。

如果需要历史资源统计，还需要 Prometheus 或其他监控系统保存时间序列数据。Kubernetes 当前 Pod 列表无法完整还原已经删除的 Pod。

## **第六部分：最终技术决策建议**

结合目前的项目阶段，我建议先确立以下规则：

| 决策项目          | 建议                                     |
| ------------- | -------------------------------------- |
| 默认 Spark 模式   | Local Mode                             |
| 交互式分布式模式      | Client Mode                            |
| 长时间独立任务       | Cluster Mode                           |
| Spark Master  | 不部署                                    |
| Spark Runtime | 统一版本，Notebook 与 Driver / Executor 保持兼容 |
| Notebook 权限   | Local 默认不授予 K8s API 权限                 |
| Client 权限     | 独立的 Driver ServiceAccount              |
| Cluster 权限    | 独立的 Driver Pod ServiceAccount          |
| Executor 权限   | 最小权限，默认不授予 Pod 创建权限                    |
| 用户隔离          | 后续每用户 Namespace                        |

**我认为最重要的架构决策是：把 Local Mode 作为 Notebook 的基础计算能力，把 Kubernetes Spark 作为可选的分布式计算能力。**

这样可以避免每个用户只是想验证一段 PySpark 代码，却必须创建 Driver Pod、Executor Pods，甚至申请 Kubernetes 权限。

另外，项目里应明确区分：

1. Notebook Runtime：提供 Python、Java、PySpark。
2. Spark Application Runtime：提供 Driver / Executor 所需的 Spark 运行环境。
3. Kubernetes Runtime：负责 Pod、调度、资源限制和用户隔离。

三者可以共享基础镜像，但职责不应混在一起。

## **官方参考资料**

1. [Apache Spark 4.3.0 Overview：安装、系统要求与 Local Mode](https://spark.apache.org/docs/latest/?utm_source=chatgpt.com)
2. [PySpark Installation：pip 安装和 Python 依赖](https://spark.apache.org/docs/latest/api/python/getting_started/install.html?utm_source=chatgpt.com)
3. [Running Spark on Kubernetes：Cluster Mode、Client Mode、RBAC](https://spark.apache.org/docs/latest/running-on-kubernetes?utm_source=chatgpt.com)
4. [Spark Kubernetes Client Mode Networking：Driver 与 Executor 网络连接](https://spark.apache.org/docs/latest/running-on-kubernetes.html?utm_source=chatgpt.com)

*版本说明：以上本地 Runtime 的 Java / Python 版本示例按当前官方 Spark 4.3.0 文档整理。正式落实到你的项目时，应先核实 `spark2026` 当前使用的 Spark 版本，再确定实际 Java 和 PySpark 版本，不建议直接升级项目运行时。*