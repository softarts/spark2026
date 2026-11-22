# Spark Platform Learning Lab — Solution Design

**文档类型：** Solution Design / Implementation Brief  
**目标读者：** 后续负责实现本实验环境的 AI Agent 或工程师  
**当前范围：** Phase 1 — Interactive Spark Development Environment  
**文档状态：** Phase 1 已实现并验证（2026-11-22），实现记录见 §13，后续阶段计划见 §14  
**核心目标：** 恢复并升级已有 Apache Spark / PySpark 平台经验，理解现代 Spark 交互式开发平台与分布式执行机制。

---

## 1. 项目目标与范围

### 1.1 项目目标

本项目不是从零学习 PySpark，也不是搭建 Hadoop 教学环境。目标是基于已有企业级 Spark 平台经验，构建一个尽量贴近现代主流数据平台开发体验的个人实验环境，重点理解：

- 用户登录后如何获得 Notebook 开发环境；
- Notebook、SparkSession、Spark Driver、Spark Executors 之间的关系；
- Spark 在 Local、Kubernetes Client、Kubernetes Cluster 模式下的执行路径；
- Spark 如何在 Kubernetes 上创建、管理和销毁 Executor；
- Python / PySpark 依赖如何在 Driver 与 Executor 之间分发；
- Spark 如何访问 S3-compatible Object Storage；
- 如何观察 Spark UI、Kubernetes Pods、日志和执行过程。

参考平台形态包括 Databricks、Cloudera Machine Learning (CML) 和 AWS EMR Studio。实验环境不追求复制这些商业平台的全部内部实现，而是复现其关键用户体验和可观察的 Spark 运行机制。

### 1.2 明确范围限制

**本项目只聚焦 Apache Spark / PySpark 及其运行平台。**

不把以下内容作为当前项目的研究或实现目标：

- Payments Technology / Payment Processing；
- Regulatory Reporting 业务流程；
- 金融业务规则或监管报送；
- 其他与 Spark 实验无直接关系的系统经验；
- Spring Batch 或围绕 Spark 构建传统微服务；
- Airflow、Control-M、Kafka；
- CI/CD、AI Engineering、Agentic AI；
- 多租户生产级治理；
- Hadoop / Hive 集群运维教学。

JD 和个人经历会作为目标岗位背景及既有能力映射材料保留，但不据此扩大实验范围。

### 1.3 设计原则

1. **贴近主流实践：** 优先采用当前仍有维护、使用广泛且适合个人实验的组件。版本和项目活跃度需在实施前核实。
2. **用户体验优先：** 以 Browser → Login → Notebook → PySpark → Execute → Observe 的用户路径组织实验。
3. **运行机制优先：** 重点研究 Notebook、Driver、Executor、Kubernetes 和 Object Storage。
4. **实验验证优先：** 不预设 Notebook Pod 就是 Driver，也不预设一定会创建独立 Driver Pod。
5. **Spark Job 保持轻量：** 使用 `wordcount.py` 等小型 Spark 应用验证运行机制，不构建 Spring Batch 风格服务。
6. **分阶段演进：** Phase 1 不引入 Delta Lake 或 Catalog；后续阶段在现有架构上增量扩展，不推翻 Phase 1。

---

## 2. 目标岗位 JD（原文）

以下内容由用户提供，作为准备背景保留。当前实验仅聚焦其中的 Spark / Databricks / PySpark 相关技术，不扩展到其他职责领域。

### Position

Senior Lead Software Engineer — Corporate and Investment Bank (CIB), Payments Technology – Regulatory Reporting, JPMorganChase.

### Job Description

Be an integral part of an agile team that's constantly pushing the envelope to enhance, build, and deliver top-notch technology products. As a Senior Lead Software Engineer at JPMorganChase within the Corporate and Investment Bank (CIB) Payments Technology – Regulatory Reporting, you are an integral part of an agile team that works to enhance, build, and deliver trusted market-leading technology products in a secure, stable, and scalable way. Drive significant business impact through your capabilities and contributions, and apply deep technical expertise and problem-solving methodologies to tackle a diverse array of challenges that span multiple technologies and applications.

### Job responsibilities

- Regularly provides technical guidance and direction to support the business and its technical teams, contractors, and vendors
- Owns end-to-end technical delivery for Regulatory Reporting initiatives; remains hands-on as a senior individual contributor
- Develops secure and high-quality production code, and reviews and debugs code written by others
- Drives adoption and governance of approved AI-assisted engineering practices across teams to improve code quality, delivery speed, and operational outcomes (e.g., AI-assisted code review/refactoring, test acceleration, release readiness, incident/root-cause analysis), while establishing measurable validation standards (secure coding, peer review, automated testing) and promoting reuse of proven patterns and automation within the SDLC/TLM toolchain
- Applies knowledge of tools within the Software Development Life Cycle toolchain, including approved AI-assisted development and automation capabilities, to improve the value realized by automation at scale
- Builds and optimizes data pipelines and reporting workloads using Databricks Lakehouse (Delta Lake, Spark SQL, PySpark)
- Drives decisions that influence the product design, application functionality, and technical operations and processes
- Partners with stakeholders to deliver controls for regulatory submissions (data quality, lineage, reconciliation, audit evidence)
- Serves as a function-wide subject matter expert in one or more areas of focus
- Actively contributes to the engineering community as an advocate of firmwide frameworks, tools, and practices of the Software Development Life Cycle

### Required qualifications, capabilities, and skills

- Bachelor's Degree in Computer Science/ Information systems/ Engineering or related disciplines
- Minimum of 8 years of hands-on software development experience delivering system design, application development, testing, and operational stability
- Advanced in one or more programming language(s) (Python required)
- Demonstrated experience leading effective use of enterprise-authorized AI-assisted software development tools within the work environment (e.g., for coding, code review, test acceleration, troubleshooting) with the ability to set team expectations for validating AI outputs for correctness, performance, and security
- Strong understanding of responsible AI use in engineering workflows, including data sensitivity considerations, secure handling of inputs/outputs, and adherence to resiliency and security expectations; experience coaching senior engineers/leads on compliant usage patterns and controls
- Ability to tackle design and functionality problems independently with little to no oversight
- Practical cloud native experience (AWS preferred)
- Hands-on experience with PySpark/Apache Spark and Databricks (Delta Lake/Lakehouse)
- Good exposure to Payments and Regulatory Reporting domains

### Preferred qualifications, capabilities, and skills

- Experience with agentic AI patterns for engineering automation and operational workflows (within approved environments)
- Experience with data quality/reconciliation frameworks for regulatory reporting
- Experience with CI/CD and automated testing for data pipelines
- Exposure to AWS data services (e.g., S3, Glue, Lambda, Step Functions, EMR)
- Exposure to observability tooling (logs/metrics/traces) for production support

---

## 3. Candidate's Existing Yitu Experience

以下为用户提供的原始英文经历，必须保持原意，不应擅自添加未确认的技术细节：

> Built an enterprise big data platform on Spark and Kubernetes, processing continuous telemetry and terabytes of facial data streams from tens of thousands of edge cameras. Designed a high-throughput ingestion pipeline utilizing PySpark and C++ SDKs to land high-velocity structured streams into partitioned Parquet tables across Hive, HDFS, and MinIO, while building low-latency query APIs to serve downstream analytics.

### 3.1 已明确提及的技术和经验

| 领域 | 用户提供的经历 |
|---|---|
| Big Data Platform | Enterprise big data platform on Spark and Kubernetes |
| Workload | Continuous telemetry and terabytes of facial data streams |
| Data Sources | Tens of thousands of edge cameras |
| PySpark | High-throughput ingestion pipeline |
| Native Integration | PySpark and C++ SDKs |
| Storage Format | Partitioned Parquet tables |
| Metadata / Table Ecosystem | Hive |
| Storage | HDFS and MinIO |
| Downstream Serving | Low-latency query APIs |

### 3.2 重要事实边界

以下历史细节尚未确认，不允许实现者或 AI Agent 将其写成既定事实：

- 当时 Notebook Pod 是否就是 Spark Driver；
- Spark 使用的具体 Deploy Mode；
- Spark 是否运行在 Kubernetes Executor Pods 中；
- 当时是否使用 Spark Standalone Master；
- Hive Metastore 的具体部署方式及 Spark Catalog 配置；
- MinIO 的具体访问协议、S3A Connector 版本和配置；
- C++ SDK 与 PySpark 的具体进程间通信方式；
- 当时 Spark、Hadoop、Hive、Java 的具体版本。

本项目会通过实验研究这些机制，但实验结果不能反向证明 Yitu 历史环境一定采用了相同实现。

---

## 4. 传统平台经验与现代目标架构的关系

用户过去的工作模式可以概括为：

```text
Browser / Notebook
       |
       v
    PySpark
       |
       v
  Spark Runtime
       |
       v
 Driver / Executors
       |
       v
 Hive Metadata + HDFS / MinIO + Parquet
```

现代目标学习方向：

```text
Browser
   |
   v
JupyterHub / Notebook
   |
   v
PySpark / Spark SQL
   |
   v
Apache Spark Runtime
   |
   +------ Driver
   |
   +------ Executors
   |
   v
S3-compatible Object Storage
   |
   v
Parquet / Delta Lake (later phase)
   |
   v
Catalog / Metadata Governance (later phase)
```

### 4.1 主要技术演进点

| 传统技术体系 | 现代目标技术体系 | 说明 |
|---|---|---|
| HDFS | S3 / S3-compatible Object Storage | 从分布式文件系统转向对象存储；访问、提交和一致性语义不同 |
| Parquet | Parquet + Delta Lake | Delta Lake 在 Parquet 数据文件基础上增加事务日志和表管理能力 |
| Hive Metastore | AWS Glue Data Catalog / Unity Catalog 等 | Catalog 与治理能力的演进；产品能力范围并不等价 |
| 自建 Notebook 平台 | Databricks / CML / EMR Studio 等托管平台 | 用户体验产品化，底层计算部署由平台管理 |
| 自行管理 Spark Runtime | Managed Spark / Spark on Kubernetes | 计算资源和生命周期管理方式可能不同 |

### 4.2 必须正确理解的概念

- Hive Metastore 主要负责表、Schema、Location、Partition 等元数据，不是 Spark SQL 执行引擎。
- Spark SQL Engine 负责 SQL 解析、优化和执行；它可以通过 Catalog / Metastore 取得表元数据。
- Delta Lake 不等于 Parquet。Delta Lake 使用 Parquet 数据文件和 `_delta_log` 事务日志实现表级事务与版本管理等能力。
- S3 与 HDFS 不等价。S3 是对象存储，Spark 通常通过 Hadoop S3A Connector 等方式访问。
- AWS Glue Data Catalog 和 Databricks Unity Catalog 有部分重叠，例如 Catalog、Database/Schema、Table Metadata，但不能简单视为同一个产品。Unity Catalog 的统一治理能力范围更广。
- Kubernetes 模式的 Spark Master URL 通常指向 Kubernetes API Server（例如 `k8s://https://...`），不是 Spark Standalone Master 服务。

---

## 5. Phase 1 Scope

### 5.1 Phase 1 目标

建立一个可登录、可使用 Notebook、可执行 PySpark、可观察 Spark 运行机制，并能访问 S3-compatible Object Storage 的个人实验平台。

**Phase 1 核心组件：**

1. Docker Desktop Kubernetes（原设计为 Minikube，实施时改用，原因见 §13.1）
2. Kubernetes
3. JupyterHub
4. User Notebook Pod
5. Apache Spark / PySpark
6. Silo（作为优先评估的 S3-compatible Object Storage 候选）
7. External GitHub
8. Persistent Volume / PVC
9. Spark UI
10. Traefik Ingress + Portal 入口页（2026-11-22 增加：统一外部入口，见 §13.2）

Phase 1 不部署 Delta Lake、Hive Metastore、AWS Glue Data Catalog 或 Unity Catalog。

### 5.2 逻辑架构

```text
                         Browser
                            |
                            v
                       JupyterHub
                            |
                            v
                  User Notebook Pod
                  - Jupyter Notebook
                  - Python
                  - PySpark
                  - Git client
                  - Local repository
                            |
                            | SparkSession / SparkContext
                            v
                     Spark Driver
                            |
                   Kubernetes API Server
                            |
                  +---------+---------+
                  |                   |
                  v                   v
             Executor Pod 1      Executor Pod N
                  |                   |
                  +---------+---------+
                            |
                            v
                  S3A-compatible access
                            |
                            v
                         Silo
                  S3-compatible API
                            |
                            v
                    Object / Bucket
```

该图表达逻辑关系，不预设 Driver 一定在 Notebook Pod 内。Driver 的实际位置由运行模式决定：

- Local Mode：Driver 和本地执行线程在 Notebook Pod 所在进程环境内。
- Kubernetes Client Mode：Driver 进程在 Notebook Pod（或提交客户端环境）中运行，Executor 由 Kubernetes 创建为 Pods。
- Kubernetes Cluster Mode：Driver 由 Spark 在 Kubernetes 中创建独立 Pod，Executor 也由 Kubernetes 创建。

具体模式能否成功运行，取决于 Spark 版本、提交方式、镜像、权限、网络和配置，必须通过实验确认。

### 5.3 组件职责

| 组件 | Phase 1 职责 |
|---|---|
| Docker Desktop Kubernetes | 本地 Kubernetes 集群（kind 模式单节点） |
| Traefik | Ingress Controller；唯一对外入口 `http://<host>:8080` |
| Portal | 静态入口页，链接 JupyterHub、Spark UI、Silo Console |
| JupyterHub | 登录入口、用户服务器生命周期管理 |
| User Notebook Pod | 用户开发环境，保存 Notebook，安装 Python 依赖，运行 PySpark |
| Apache Spark | 分布式计算引擎 |
| Spark Driver | Spark 应用协调、计划和任务调度 |
| Spark Executors | 执行分配的 Tasks、缓存数据并向 Driver 汇报 |
| Silo | 提供 S3-compatible Object Storage API |
| Git | Notebook Pod 内的本地版本控制客户端 |
| GitHub | 外部远程代码仓库，不部署在集群内 |
| PVC | Notebook 工作目录持久化 |
| Spark UI | 观察 Jobs、Stages、Tasks、Executors、Storage 等信息 |

### 5.4 Silo 选择说明

MinIO 的社区版维护与发行策略发生变化，因此本设计不把 MinIO 作为默认长期选择。

Silo（PGSTY 维护的 MinIO 社区分支）作为优先评估候选，主要理由是其目标与 MinIO 兼容性和迁移便利性相关，适合降低 Spark S3A 实验的变量。

项目入口：<https://github.com/pgsty/silo>

但不得假设它在所有 S3 API、版本、部署模式和生产场景中都完全兼容 MinIO 或 AWS S3。实现前应检查：

- 最近发布版本和维护活动；
- 容器镜像来源与安全性；
- License；
- S3 API 覆盖范围；
- Bucket、Object、Multipart Upload 等基本功能；
- Spark Hadoop S3A Connector 所需 API 的兼容性；
- Kubernetes 部署文档和资源需求。

如果 Silo 的维护状态或兼容性无法通过核实，应暂停选型并报告，不要静默替换成未经评估的产品。

其他可研究候选包括 RustFS、SeaweedFS、Garage，但 Phase 1 不需要同时部署多个对象存储产品。

### 5.5 Git 与 GitHub

无需在 Kubernetes 集群内部署 Git Server 或 GitHub Enterprise。

- Git：安装在 User Notebook Pod 中，负责本地版本控制。
- GitHub：使用外部远程仓库，保存代码和 Notebook 版本。
- PVC：负责 Notebook 工作目录和本地文件的持久化。
- GitHub：不替代 PVC；PVC 也不替代版本控制。

建议工作流：

```bash
git clone https://github.com/<account>/<repository>.git
cd <repository>

# Edit notebooks and PySpark scripts

git add .
git commit -m "Add Spark WordCount experiment"
git push
```

认证建议使用 SSH Key 或 GitHub Fine-grained Personal Access Token。凭证应通过安全方式注入，不得硬编码到 Notebook、脚本或 Git 仓库。

GitHub 仓库可采用如下结构：

```text
spark-lab/
├── README.md
├── notebooks/
│   ├── 01_wordcount_local.ipynb
│   ├── 02_wordcount_k8s_client.ipynb
│   └── 04_wordcount_s3a.ipynb
├── jobs/
│   └── wordcount.py
├── data/
│   └── sample.txt
├── requirements.txt
└── docs/
    └── architecture.md
```

不要将大型数据集、访问凭证、密钥或生成的大型输出文件提交到 Git。

---

## 6. WordCount Validation Plan

WordCount 是 Phase 1 的核心端到端验证程序，不是为了从零学习 Spark API，而是用于确认 Notebook、Driver、Executor、Kubernetes 和 Object Storage 的运行关系。

必须按以下顺序逐步验证。

### 6.1 WordCount 1 — Local Mode

示例：

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WordCount-Local")
    .master("local[*]")
    .getOrCreate()
)

text = spark.sparkContext.parallelize(
    ["spark kubernetes", "spark notebook", "pyspark spark"],
    2
)

counts = (
    text.flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
)

print(sorted(counts.collect()))
```

验证：

- Notebook 可以创建 SparkSession；
- Spark Job 成功执行；
- Driver 进程的位置；
- Local 模式下执行线程是否位于 Notebook Pod；
- Spark UI 是否可访问；
- Kubernetes 中是否创建额外 Executor Pods（预期 Local Mode 不会创建独立 Kubernetes Executor Pods，但必须观察确认）。

### 6.2 WordCount 2 — Kubernetes Client Mode

目标：Notebook Pod 内运行 Driver，Kubernetes 中运行 Executor Pods。

验证：

- Driver 是否确实在 Notebook Pod；
- Executor Pods 是否由 Kubernetes 创建；
- Driver 与 Executor 的网络通信；
- Executor Pod 的生命周期；
- Spark UI 中 Executor 信息；
- Kubernetes Pod、Events 和日志与 Spark UI 的对应关系。

注意：需要正确配置 Kubernetes API 访问、RBAC、Driver Service Account、Driver 可被 Executor 访问的网络地址和端口、镜像、资源请求与限制。不要仅凭 `spark.master` 字符串判断运行模式。

### 6.3 WordCount 3 — Kubernetes Cluster Mode

使用独立 Spark 应用脚本，例如 `jobs/wordcount.py`，通过 `spark-submit` 提交。

目标：Driver 由 Kubernetes 创建为独立 Pod，Notebook 负责编辑和提交，而不是承担 Driver 进程。

验证：

- 提交后是否创建独立 Driver Pod；
- Driver Pod 的名称、镜像和 Service Account；
- Executor Pods 是否由 Driver 管理；
- Job 结束后的 Pod 清理策略；
- 日志与 Spark UI 的可观察性；
- Notebook 与 Driver 的生命周期是否独立。

不开发常驻服务，不引入 Spring Batch。

### 6.4 WordCount 4 — Silo / S3A Integration

将输入文件放入 Silo 的 Bucket，使用 Spark S3A Connector 读取；将 WordCount 结果写回对象存储。

逻辑路径示例：

```text
s3a://spark-data/input/
s3a://spark-data/output/
```

验证：

- Silo S3 Endpoint 可从 Notebook Pod 访问；
- Silo S3 Endpoint 可从 Executor Pods 访问；
- Spark S3A Connector 与 Hadoop 依赖版本兼容；
- Access Key / Secret Key 的安全注入；
- Path-style access、Endpoint、SSL 等配置是否需要；
- Spark 能够读取对象；
- Spark 能够写入结果对象；
- 输出对象能在 Silo 侧确认；
- 不能只验证 Driver 可访问对象存储，还必须验证 Executor 的数据读写路径。

### 6.5 依赖传播验证

明确区分：

- Notebook Pod 的 Python 环境；
- Driver Python 环境；
- Executor Python 环境；
- Spark / Hadoop JVM 依赖；
- S3A Connector 依赖。

验证用户在 Notebook Pod 执行 `pip install` 后，Executor 是否能够 import 相同 Python 包。不要假设 pip 安装会自动传播到 Executor。

记录至少一种可行依赖交付方式，例如统一 Spark 镜像、Python environment packaging、`--py-files` 或 Spark 支持的其他机制。最终方案需根据 Spark 版本和运行模式验证。

---

## 7. Phase 1 Acceptance Criteria

以下全部为验收项目；每项必须有可复现的证据（命令输出、Pod 信息、日志、Spark UI 截图或架构说明）。

状态更新于 2026-11-22。`[x]` 表示已有实测证据（证据列给出出处；README 指 README.md 的章节），`[ ]` 表示尚未完成。

### User Experience

| | 项目 | 证据 |
|---|---|---|
| [x] | Kubernetes 集群正常运行（Docker Desktop，替代 Minikube） | `kubectl get nodes`：`desktop-control-plane Ready v1.36.1`；`startcluster.sh` 可从 Docker 未启动状态冷启动（约 57 秒） |
| [x] | 用户可以登录 JupyterHub | 通过 `http://<LAN IP>:8080/hub/login` 登录：返回 302 跳转到 `/hub/spawn` |
| [x] | JupyterHub 能创建 User Notebook Pod | 创建了 `jupyter-spark`，使用镜像 `spark-lab-runtime:0.3.0`；`/user/spark/lab` 返回 200 |
| [x] | Notebook 内容在 Pod 重建后持久化（PVC） | 写入 `work/persist-check.txt`，执行 `stopcluster.sh` 和 `startcluster.sh` 后重新 spawn，文件内容仍为 `keep-me` |
| [x] | Notebook Pod 可以访问外部 GitHub | 在 Pod 中 `git clone https://github.com/octocat/Hello-World.git` 成功 |
| [ ] | 可以 Clone、Commit、Push | Clone 和 Commit 已验证；Push 需要用户自己的 GitHub 凭证（SSH Key 或 Fine-grained PAT），尚未执行 |
| [x] | Notebook 中可以运行 PySpark | 用 `nbconvert --execute` 执行两个示例 Notebook，均成功（README §3.2、§3.3） |

### Spark Local Mode

| | 项目 | 证据 |
|---|---|---|
| [x] | 创建 SparkSession | `master: local[*]` |
| [x] | WordCount Local Mode 成功 | `[('kubernetes', 1), ('notebook', 1), ('pyspark', 1), ('spark', 3)]` |
| [x] | 可以访问 Spark UI | 运行期间 `/spark-ui/jobs/` 经 Ingress 返回 200，页面链接带 `/spark-ui/` 前缀 |
| [x] | 确认 Driver 进程位置 | Driver 在 `jupyter-spark` Pod 中，是 kernel 通过 Py4J 启动的 JVM |
| [x] | 确认执行进程和线程所在位置 | task 中读取的 host 都是 `jupyter-spark`；Python worker PID（144/145）与 kernel PID（35）不同；Driver 看到的 executor 数为 1，即 Driver 自身；Kubernetes 中没有 Executor Pod |

### Spark Kubernetes Client Mode

| | 项目 | 证据 |
|---|---|---|
| [x] | WordCount Client Mode 成功 | `02_wordcount_executor.ipynb` 输出完整的计数结果 |
| [x] | 确认 Driver 位置 | `deployMode: client`，Driver 在 Pod `jupyter-spark` 中 |
| [x] | 观察到 Kubernetes Executor Pods | `wordcount-k8s-client-…-exec-1/2` 状态为 Running |
| [x] | Spark UI Executor 信息与 Pods 对应 | task 中读取的主机名就是 Executor Pod 名；REST API `/executors` 返回的 `hostPort` 10.244.0.34/35:7079 与 Pod IP 一致 |
| [x] | Executor 结束后的资源清理 | `spark.stop()` 后 Executor Pod 被删除。修复 RBAC 之前，Pod 停留在 Completed 状态，并报 `deletecollection` 403（见 §13.4） |

### Spark Kubernetes Cluster Mode

| | 项目 | 证据 |
|---|---|---|
| [x] | 独立 `wordcount.py` 可提交 | `spark-submit --deploy-mode cluster local:///opt/spark-lab/examples/jobs/wordcount.py`，作业 `phase: Succeeded` |
| [x] | Kubernetes 创建独立 Driver Pod | `wordcount-cluster-…-driver`，ServiceAccount 为 `spark-client`，镜像为 `spark-lab-runtime:0.3.0` |
| [x] | 观察 Executor Pods | Driver Pod 创建了 `wordcount-kubernetes-cluster-…-exec-1/2` |
| [x] | 应用结束后的 Pod 生命周期 | Executor Pod 被删除；Driver Pod 保留为 Completed，可查看日志；`/spark-driver/` 在 Driver 运行期间返回 200，结束后返回 503 |
| [x] | Notebook 与 Driver 的关系 | Notebook 只运行 `spark-submit` 客户端，客户端等待作业结束后退出；Driver 是另一个 Pod，Notebook 停止也不影响它 |

### Object Storage

| | 项目 | 证据 |
|---|---|---|
| [x] | Silo 正常运行 | `silo version RELEASE.2026-11-12T00-00-00Z`，Console 位于 `/silo/` |
| [x] | Bucket 创建成功 | `startcluster.sh` 中执行 `mcli mb --ignore-existing local/spark-data` |
| [x] | Spark Driver 可访问 Silo | `test_s3a.py`（Local Mode）读写成功 |
| [x] | Spark Executors 可访问 Silo | Cluster Mode：Executor 读取 `s3a://spark-data/input/sample.txt` 并写入 `cluster-wordcount/output/part-*.json` |
| [x] | Spark S3A 读取成功 | 同上 |
| [x] | Spark S3A 写入成功 | 写出 Parquet（`test_s3a.py`）和 JSON（Cluster Mode） |
| [x] | 输出对象可从 Silo 侧确认 | Console Bucket 列表；`scripts/silo-export.sh` 导出的 JSON 内容为 `{"word":"spark","count":3}`，Parquet 文件首尾为 `PAR1` |

### Final Documentation

| | 项目 | 出处 |
|---|---|---|
| [x] | 最终部署架构图 | §13.3 |
| [x] | Local / Client / Cluster 执行链路图 | §13.3 |
| [x] | Notebook 是否就是 Driver | §13.6 Q1 |
| [x] | Driver 何时创建 | §13.6 Q2 |
| [x] | Driver 如何创建和管理 Executor | §13.6 Q3 |
| [x] | Executor 何时销毁 | §13.6 Q4 |
| [x] | Python 依赖如何提供给 Executor | §13.6 Q5（含实测） |
| [x] | Interactive 与 Batch 的区别 | §13.6 Q6 |
| [x] | 实际组件版本和关键配置 | README §5，§13.5 |

---

## 8. Phase 1 Final Deliverables

Phase 1 的主要产出不是单纯的代码仓库，而是经实验验证的架构理解和运行机制文档。

必须交付：

1. **可运行的实验环境**
   - Docker Desktop Kubernetes（原为 Minikube）
   - JupyterHub
   - User Notebook Pod
   - Spark / PySpark
   - Silo（前提是通过兼容性和维护状态评估）
   - PVC
2. **WordCount 实验**
   - Local Mode
   - Kubernetes Client Mode
   - Kubernetes Cluster Mode
   - S3A / Silo Integration
3. **架构图**
   - 总体部署架构
   - Local Mode 执行链路
   - Client Mode 执行链路
   - Cluster Mode 执行链路
4. **运行机制说明**
   - Notebook 与 Driver 的关系
   - SparkSession 创建过程
   - Driver 与 Executor 的关系
   - Kubernetes Pod 生命周期
   - Python 依赖传播
   - Spark UI 与 Kubernetes 观察方式
5. **版本与配置清单**
   - Kubernetes / Docker Desktop
   - JupyterHub / Jupyter Server
   - Spark / PySpark
   - Java
   - Python
   - Hadoop / Hadoop AWS / AWS SDK（如使用）
   - Silo 镜像与版本
   - 容器镜像与关键 Spark 配置

---

## 9. Future Phases — Incremental Extension Only

后续阶段必须建立在 Phase 1 已运行的环境上，不应为了加入新组件而重构 Phase 1 的基本用户体验。

**2026-11-22：** 基于已实现环境的可行性评估和具体计划见 §14。

### Phase 2 — Spark on Kubernetes Deep Dive

重点：

- Client Mode 与 Cluster Mode；
- Driver / Executor Pod；
- Kubernetes RBAC；
- Resource Requests / Limits；
- Spark Dynamic Allocation（在适用配置下）；
- Executor 生命周期；
- Spark UI、日志与 Kubernetes 事件；
- Python 依赖分发。

### Phase 3 — Object Storage and Parquet

重点：

- S3 / S3-compatible Object Storage；
- Hadoop S3A Connector；
- Parquet 读写；
- Partitioning；
- Small Files；
- 文件提交与并发写入；
- Object Storage 与 HDFS 的语义差异；
- Spark 读写性能和数据布局。

### Phase 4 — Delta Lake

在现有 Spark + Object Storage 基础上增量加入 Delta Lake。

重点：

- Delta Transaction Log（`_delta_log`）；
- ACID Transactions；
- Schema Enforcement / Evolution；
- MERGE；
- UPDATE / DELETE；
- Time Travel；
- Optimistic Concurrency Control；
- Parquet 数据文件与 Delta Log 的关系。

### Phase 5 — Catalog / Metadata

研究并比较：

- Hive Metastore；
- AWS Glue Data Catalog；
- Databricks Unity Catalog。

重点理解：

- Catalog、Schema、Table；
- External / Managed Table；
- Table Location；
- Metadata 与数据文件的关系；
- Spark 如何解析表名并访问底层数据；
- Catalog 的权限和治理能力差异。

Glue Data Catalog 与 Unity Catalog 不应简单视为同一种产品。Glue Data Catalog 主要是 AWS 数据服务生态中的元数据目录；Unity Catalog 是 Databricks 的统一数据治理方案，覆盖更广泛的治理和访问控制能力。

Phase 1 不部署任何上述 Catalog。

---

## 10. Implementation Constraints and Instructions to AI Implementer

实现者应遵守以下约束：

1. **先核实版本兼容性，再安装。** 不要随意选择 Spark、Java、Python、Hadoop AWS、AWS SDK 和 S3A Connector 版本。
2. 选择 Spark 版本时，应检查对应 Spark Kubernetes 文档、Python 版本支持、Java 版本要求及容器镜像要求。
3. 不要假设所有 Spark 部署模式都能通过同一 Notebook 配置完成；Client Mode 和 Cluster Mode 的提交方式可能不同。
4. 不要把 Spark Standalone Master/Worker 当作 Kubernetes 模式的必需组件。
5. 不要把 Hive Metastore、Glue Catalog、Unity Catalog 或 Delta Lake 提前加入 Phase 1。
6. 不要在 Phase 1 部署 Airflow、Kafka、Control-M 或其他无关服务。
7. 不要创建 Spring Boot / Spring Batch 风格的 Spark Job Service。
8. WordCount 只作为执行机制验证，不把项目变成 Spark 入门教程。
9. GitHub 使用外部托管服务；集群内只安装 Git 客户端。
10. Notebook 持久化使用 PVC；不要误以为 GitHub 能替代 Notebook 工作目录持久化。
11. Silo 的维护状态、License、镜像来源和 S3 API 兼容性必须在实施前核实；不能把社区分支描述为 MinIO 官方产品或官方维护版本。
12. Spark S3A 测试必须确认 Driver 和 Executor 均能访问对象存储。
13. Python 包安装在 Notebook Pod 后，不得假设 Executor 自动拥有这些包；必须实际验证并记录依赖交付机制。
14. 对所有未确认的历史 Yitu 部署细节，明确标记为“未知 / 待验证”，不得推断为事实。
15. 每完成一个阶段，先提供部署结果、验证证据和问题清单，再继续下一阶段。
16. 不要为了追求“生产级完整”而扩大 Phase 1。优先保证最小架构可运行、行为可观察、结论可验证。

---

## 11. Open Questions to Resolve During Implementation

以下问题不应在没有证据时提前给出确定答案：

- 选定 Spark 版本后，Kubernetes Client Mode 和 Cluster Mode 的具体启动参数是什么？
- JupyterHub 使用何种 Authenticator？个人实验可以采用简单认证方式，但不得将其描述为企业生产安全配置。
- Notebook Pod 的 PVC 使用哪种 StorageClass？
- Spark Driver Pod 是否需要额外 Service / headless service 或特定网络配置？
- 节点资源是否足够同时运行 JupyterHub、Notebook、Spark Driver、多个 Executors 和 Silo？
- Silo 当前发布、维护和 S3 API 兼容性是否满足实验需求？
- Spark S3A Connector 对应的 Hadoop AWS 与 AWS SDK 版本组合是什么？
- Spark UI 通过何种方式从本地浏览器访问？
- Cluster Mode 的 Driver Pod 如何获得 Silo 凭证？
- Notebook Pod、Driver Pod、Executor Pods 的 Python 环境如何保持兼容？

所有问题应通过官方文档、版本矩阵和实际实验解决，并在实现文档中记录来源和结论。

**2026-11-22：** 以上问题的实测结论见 §13.5。

---

## 12. Definition of Success

Phase 1 成功的定义不是“Kubernetes 里安装了几个组件”，而是：

> 用户能够登录 JupyterHub，获得持久化 Notebook 开发环境，在 Notebook 中编写和运行 PySpark WordCount；能够分别验证 Local Mode、Kubernetes Client Mode 和 Kubernetes Cluster Mode；能够通过 Spark UI 与 Kubernetes 观察 Driver 和 Executor 的实际位置与生命周期；并能够让 Spark Driver 和 Executors 通过 S3A 访问 Silo 中的对象数据。

最终必须基于实际实验回答：

1. Databricks / CML 等交互式平台中，Notebook 是否就是 Driver？答案是否取决于运行模式和平台实现？
2. Driver 是什么时候创建的？
3. Driver 如何创建和管理 Executor？
4. Executor 什么时候销毁？
5. 用户 Python 依赖如何提供给 Executor？
6. Interactive Notebook 模式和 Batch Spark 模式有什么区别？
7. Local、Kubernetes Client、Kubernetes Cluster 三种执行路径有何区别？

**项目主线：从传统 Spark + Kubernetes + Hive/HDFS/Parquet 经验，升级到现代 Spark + Kubernetes + S3-compatible Object Storage，并在后续阶段引入 Delta Lake 与 Catalog。**

---

## 13. Phase 1 Implementation Record（2026-11-22）

本节记录 Phase 1 的实际实现、与原设计的偏差，以及实验结论。操作说明见 README.md。

### 13.1 平台偏差：Docker Desktop Kubernetes 替代 Minikube

| 项目 | 结论 |
|---|---|
| 实际平台 | Docker Desktop 内置 Kubernetes，kind 模式单节点 `desktop-control-plane`。Kubernetes v1.36.1，containerd 2.3.1，arm64，节点可分配 18 CPU / 16 GiB |
| 原因 | Apple Silicon 原生 arm64，不需要额外的虚拟机层；环境中已经在用 |
| 影响 1 | 节点有自己的 containerd，`docker build` 出的镜像 Pod 看不到（实测 `ErrImageNeverPull`）。`startcluster.sh` 用一个临时特权 Pod 执行 `ctr images import`，并按构建输入哈希跳过重复导入 |
| 影响 2 | LoadBalancer Service 被 Docker Desktop 发布在 Mac 所有网卡上（实测 `*:8080`），局域网可以直接访问 |
| 影响 3 | 节点没有挂载 Mac 的 `/Users`（实测 hostPath 报 `not a directory`），PVC 数据只在虚拟机磁盘镜像 `Docker.raw` 中 |

### 13.2 统一入口与一键启停（2026-11-22 新增）

需求：只用一个启动脚本；浏览器访问一个固定端口；不使用 port-forward；支持从其他机器访问；提供统一入口页。

| 设计点 | 实现 |
|---|---|
| Ingress Controller | Traefik（chart 42.4.1，Proxy v3.7.19）。社区版 ingress-nginx 已于 2026-03 退役，不再选用 |
| 对外端口 | Traefik 的 LoadBalancer Service，端口 8080 |
| 路由 | `/` Portal；`/hub`、`/user` JupyterHub；`/spark-ui` Notebook Driver UI；`/spark-driver` Cluster Mode Driver UI；`/silo` Silo Console（`k8s/50-ingress.yaml`） |
| Spark UI 子路径 | 用 Traefik Middleware 去掉前缀，并加上 `X-Forwarded-Context`，页面链接据此生成。302 跳转还需要 `spark.ui.proxyRedirectUri=/`，已写入镜像的 `spark-defaults.conf` |
| Cluster Driver UI | Service `spark-cluster-driver-ui` 的 selector 为 `spark-role=driver`。Completed 状态的 Pod 不是 Ready，不会接收流量 |
| Silo Console 子路径 | `MINIO_BROWSER_REDIRECT_URL=http://localhost:8080/silo`；Console 不检查主机名，局域网 IP 也能登录 |
| 启动 | `startcluster.sh`：自动启动 Docker Desktop → 镜像（按需构建和导入）→ 凭证 → Silo 和 Bucket → Portal → Traefik → JupyterHub → 健康检查 |
| 停止 | `stopcluster.sh` 把副本数缩到 0，保留 PVC；`--purge` 删除全部组件和数据 |
| 凭证 | `.lab/credentials.env`（git-ignored，第一次运行时随机生成）。JupyterHub 密码不再写在 `values.yaml` 中 |
| 示例分发 | 镜像中的 `/opt/spark-lab/examples` 由 init container 在每次启动时复制到 PVC 的 `work/examples` |
| 精简 | 关闭了 Z2JH 的 user-scheduler、continuous-image-puller 和 user-placeholder。它们用于多节点集群，单节点环境中没有作用 |

### 13.3 部署架构与执行链路

**部署架构：**

```text
Browser (Mac / LAN)  ──http://<host>:8080──►  Docker Desktop (*:8080)
                                                   │
┌──────────────────────── Kubernetes: desktop-control-plane ─────────────────────────┐
│  ns traefik:   traefik (Ingress Controller, LoadBalancer :8080)                    │
│                   │ /        /hub,/user        /spark-ui        /spark-driver  /silo│
│  ns spark-lab:    ▼            ▼                  ▼                 ▼            ▼   │
│               portal      proxy-public ──► hub   spark-client-   spark-cluster-  silo│
│               (nginx)     (CHP)          (KubeSpawner)  driver:4040  driver-ui:4040 :9001│
│                              │                  │                 │          :9000│
│                              ▼                  ▼                 ▼            ▲  │
│                        jupyter-spark  ◄── PVC claim-spark   *-driver Pods     │  │
│                        (Notebook; Local/Client Driver)                        │  │
│                              │  K8s API (SA spark-client)                     │  │
│                              ▼                                               │  │
│                        *-exec-N Executor Pods ──────── S3A ──────────────────┘  │
│                                                   silo ◄── PVC silo-data         │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Local Mode：**

```text
jupyter-spark Pod
└── Jupyter kernel (Python) ──Py4J──► Driver JVM
                                        ├── task threads (local[*])
                                        └── Python workers (fork) 执行 lambda
Kubernetes 中没有 Executor Pod
```

**Kubernetes Client Mode：**

```text
jupyter-spark Pod                              Kubernetes API
└── kernel ──Py4J──► Driver JVM ──create pods──►  (SA spark-client)
         ▲                │                          │
         │ 7078 RPC / 7079 BlockManager               ▼
         └──── spark-client-driver.spark-lab.svc ◄── Executor Pod 1..N
                (headless Service)                    └── S3A ──► silo:9000
```

**Kubernetes Cluster Mode：**

```text
jupyter-spark Pod: spark-submit (客户端，等待作业结束后退出)
      │ create Driver Pod + Driver Service
      ▼
<name>-driver Pod (Driver JVM + Python) ──create pods──► Executor Pod 1..N
      │ 作业结束：删除 Executors，Driver Pod 保留为 Completed          └── S3A ──► silo:9000
```

### 13.4 实施中发现的问题与修复

| 问题 | 现象 | 修复 |
|---|---|---|
| 节点看不到本地镜像 | init container 报 `ErrImageNeverPull` | 导入节点 containerd（§13.1） |
| 新 PVC 属主是 root | 原 README 要求手动 `chown` | init container 以 root 运行并 `chown`；`WORKDIR` 改为 `/home/jovyan`，这个目录一定存在 |
| Spark 4.1 stop 时批量删除资源 | `cannot deletecollection services` 和 `persistentvolumeclaims`（403），Executor Pod 停留在 Completed 状态 | Role 增加 `deletecollection` 权限和 `persistentvolumeclaims` 资源 |
| 固定的 `podNamePrefix` | 连续运行时报 `pods "…-exec-1" already exists`（409） | 去掉这个配置，使用 Spark 默认的唯一前缀 |
| Spark UI 根路径跳转丢失前缀 | `/spark-ui/` 跳转到 `/jobs/`，返回 404（用户反馈） | 镜像中的 `spark-defaults.conf` 设置 `spark.ui.proxyRedirectUri /`。已实测 Local Mode 和 Cluster Mode 的跳转都带上前缀 |
| Traefik chart 有严格 schema | 未知字段 `logs` 导致安装失败 | 删除该字段（chart 42.x 默认不开 access log） |

### 13.5 §11 Open Questions 的结论

| 问题 | 结论 |
|---|---|
| Client 和 Cluster Mode 的启动参数 | README §3.3（SparkSession 配置）和 §3.5（`spark-submit`） |
| Authenticator | DummyAuthenticator，随机密码。仅用于个人学习，不是生产配置 |
| StorageClass | `standard`（rancher.io/local-path）；该存储类忽略 `fsGroup`，需要用 init container 修正属主 |
| Driver 网络 | Client Mode 需要 headless Service 加固定的 7078/7079 端口；Cluster Mode 由 Spark 自动创建 Driver Service |
| 节点资源 | 18 CPU / 16 GiB。空闲时所有组件的 requests 约 1 CPU / 370 MiB；每个 Executor 约 1.4 GiB（1g 加 overhead） |
| Silo | `RELEASE.2026-11-12T00-00-00Z`，AGPLv3，由 PGSTY 维护；S3A 读写和 Console 正常 |
| S3A 版本组合 | hadoop-aws 3.4.2，AWS SDK v2 bundle 2.29.52 |
| Spark UI 访问 | Ingress 子路径 `/spark-ui/` 和 `/spark-driver/`；不需要 port-forward |
| Cluster Driver 的 Silo 凭证 | `spark.kubernetes.driver.secretKeyRef.*` 从 Secret 注入（已验证），不出现在命令行上 |
| Python 环境一致性 | Notebook、Driver、Executor 使用同一个镜像（Python 3.10.12，实测一致） |

### 13.6 Definition of Success 的实验回答

1. **Notebook 是否就是 Driver？** 取决于运行模式。在本实验中，Local 和 Client Mode 下 Driver 就是 Notebook kernel 启动的 JVM，位于 Notebook Pod 中。Cluster Mode 下 Notebook 只是提交客户端，Driver 在独立的 Pod 中。Databricks 和 CML 的内部拓扑没有在本实验中验证，不能用本实验的结果推断。
2. **Driver 何时创建？** Local 和 Client Mode：Notebook 中第一次执行 `SparkSession.builder.getOrCreate()` 时，在 kernel 进程中启动 Driver JVM。Cluster Mode：`spark-submit` 调用 Kubernetes API 创建 Driver Pod。
3. **Driver 如何创建和管理 Executor？** Driver 中的 `KubernetesClusterSchedulerBackend` 通过 ServiceAccount `spark-client` 调用 Kubernetes API 创建 Executor Pod，并 watch Pod 状态。Executor 启动后回连 Driver 注册（Client Mode 经过 headless Service）。Client Mode 的 Executor Pod 通过 `spark.kubernetes.driver.pod.name` 设置 ownerReference 指向 Notebook Pod，Notebook Pod 被删除时 Executor Pod 也会被回收。
4. **Executor 何时销毁？** 在 `spark.stop()` 时，或 Cluster Mode 作业结束时，Driver 主动删除 Executor Pod（需要 `delete` 和 `deletecollection` 权限）。kernel 一直存活且没有调用 `stop()` 时，Executor 会一直占用资源。Phase 2 的 Dynamic Allocation 会研究空闲时回收 Executor。
5. **Python 依赖如何提供给 Executor？** 实测：在 Notebook 中 `pip install --user tabulate`，包安装在 PVC 的 `~/.local` 下，Driver 能 import，Executor 报 `ImportError`（Executor 不挂载 Notebook 的 PVC）。`sc.addPyFile()` 加入单个 `.py` 文件不能解决包目录的问题；把包目录打成 zip 再 `addPyFile("deps.zip")` 后，Executor import 成功（`./deps.zip/tabulate/__init__.py`）。结论：运行时镜像是基线；纯 Python 包可以用 zip 分发（`addPyFile` 或 `--py-files`）；带原生扩展的包需要放进镜像，或用 `--archives` 分发打包好的 Python 环境（Phase 2 验证）。
6. **Interactive 与 Batch 的区别：** Interactive（Local 或 Client Mode）中 Driver 的生命周期等于 kernel 的生命周期，Executor 在 Session 存活期间一直保留，适合探索，但空闲时也占用资源。Batch（Cluster Mode）中 Driver 的生命周期等于一次作业，作业结束后资源释放，Driver Pod 保留日志，与 Notebook 互不影响。
7. **三种执行路径的区别：** 见 §13.3。区别在于 Driver 所在的进程和 Pod、Executor 是否是独立的 Pod，以及 Executor 回连 Driver 的网络路径。

---

## 14. Phase 2–5 可行性评估与计划（2026-11-22）

评估依据：当前环境（Spark 4.1.0、Hadoop 3.4.2、单节点 18 CPU / 16 GiB、Silo、镜像中没有 Delta、pyarrow、pandas 和 spark-hadoop-cloud），以及截至 2026-11 的公开版本信息。所有新增组件都要通过 `startcluster.sh` 一键启动，并在 Portal 上增加入口，不改变 Phase 1 的使用方式。"现在可做"表示不依赖外部账号、不需要新的版本决策；"需核实"表示动手之前还要做版本或兼容性验证。

### 14.1 总览

| Phase | 现在可做 | 需要新增的组件 | 需要用户决策或外部资源 |
|---|---|---|---|
| 2 Spark on K8s Deep Dive | 全部核心项 | Spark History Server（同一个镜像） | Spark Operator 是否纳入（可选） |
| 3 Object Storage & Parquet | 全部核心项 | 镜像增加 pyarrow、pandas、`spark-hadoop-cloud` | 无 |
| 4 Delta Lake | 全部核心项 | 镜像增加 Delta Lake 4.1.x JAR 和 `delta-spark` | 无 |
| 5 Catalog | Hive Metastore（内嵌 Derby，然后独立服务）、Unity Catalog OSS（需核实） | HMS（加 PostgreSQL）、UC OSS server | AWS Glue 需要 AWS 账号，并会产生费用 |

### 14.2 Phase 2 — Spark on Kubernetes Deep Dive

| # | 实验 | 做法 | 验证 |
|---|---|---|---|
| 2.1 | Spark History Server | 用同一个运行时镜像运行 `start-history-server.sh`，读取 `s3a://spark-logs/`；作业设置 `spark.eventLog.enabled=true`。Ingress 路径 `/spark-history/`，加入 Portal | 作业结束后仍能查看 UI。这解决了 §2.2 中"UI 随 Driver 消失"的限制 |
| 2.2 | Dynamic Allocation | `spark.dynamicAllocation.enabled=true` 配合 `shuffleTracking.enabled=true`（Kubernetes 上没有 External Shuffle Service） | 观察空闲 Executor 被回收、有负载时扩容；对照 Kubernetes Events |
| 2.3 | Requests / Limits / Overhead | 调整 `spark.executor.memory`、`memoryOverhead`、`request.cores`、`limit.cores` | 对比 Pod spec 与 Spark 配置；故意制造 OOMKilled，观察 Driver 如何重建 Executor |
| 2.4 | Pod Template | `spark.kubernetes.executor.podTemplateFile` 设置标签、节点亲和性和 sidecar | `kubectl get pod -o yaml` 中可以看到合并结果 |
| 2.5 | 故障与生命周期 | 手动删除 Executor Pod 或 Driver Pod；在 Client Mode 中重启 kernel | Driver 的重试行为；ownerReference 的级联回收 |
| 2.6 | Python 依赖分发 | 打包 Python 环境（venv-pack），通过 `--archives env.tar.gz#env` 和 `PYSPARK_PYTHON=./env/bin/python` 分发；与 zip 方式和镜像方式对比 | 用带原生扩展的包验证 Executor 能否 import |
| 2.7 | 可观察性 | 对照 Spark UI、REST API、`kubectl logs` 和 Events | 写成对照表 |
| 2.8（可选） | Spark Connect | Spark 4.x 的 Spark Connect Server 单独作为一个 Pod 运行，Notebook 只作为瘦客户端 | 第三种拓扑："Notebook 既不是 Driver，也不是提交客户端"。这与 Databricks Connect 的模型相关，需要实验确认 |
| 2.9（可选） | Spark Operator | `SparkApplication` CRD 声明式提交 | 需要先核实 Kubeflow spark-operator 对 Spark 4.1 的支持情况，再决定是否纳入 |

资源：History Server 约 1 GiB；其余实验复用现有 Executor 配额。

### 14.3 Phase 3 — Object Storage and Parquet

| # | 实验 | 做法 | 验证 |
|---|---|---|---|
| 3.0 | 镜像补充 | 加入 pyarrow 和 pandas（Arrow 加速 `toPandas`，Pandas UDF）；加入 `spark-hadoop-cloud_2.13-4.1.0`（S3A committer 绑定类） | 版本与 Spark 4.1 一致 |
| 3.1 | 观察 S3A 实际发出的 S3 请求 | Silo 镜像自带的 `mcli admin trace` 可以实时显示 LIST、HEAD、PUT、COPY、DELETE | 看到 FileOutputCommitter 提交时 rename 在 S3 上变成 COPY 加 DELETE |
| 3.2 | Committer 对比 | 默认 FileOutputCommitter 与 S3A Magic Committer 对比 | 请求数量和耗时；`_SUCCESS` 文件的内容（Magic Committer 会写入 JSON 清单） |
| 3.3 | Partitioning | 用 `partitionBy` 写入合成数据集 | 目录布局；分区裁剪的效果（`explain`、读取文件数） |
| 3.4 | Small Files | 对比 `repartition` 与 `coalesce`，`maxRecordsPerFile` | 文件数量与读取耗时 |
| 3.5 | 并发写入 | 两个作业同时 overwrite 同一路径 | 观察对象存储上没有原子 rename 带来的后果，为 Phase 4 做铺垫 |
| 3.6 | 与 HDFS 的语义差异 | 整理成文档：rename、目录、列表一致性 | 引用 Hadoop S3A 官方文档 |

数据：只使用 Spark 生成的合成数据，不使用业务数据。磁盘：虚拟机还有约 402 GB 可用空间；local-path 不强制执行 PVC 的 5Gi 容量。

### 14.4 Phase 4 — Delta Lake

版本：Delta Lake 4.1.0 默认基于 Spark 4.1.0 构建（[Delta 4.1.0 release](https://github.com/delta-io/delta/releases/tag/v4.1.0)）；之后的 4.4.x 同时支持 Spark 4.2、4.1 和 4.0.1（[Databricks 2026 摘要](https://answers.databricks.com/state-data-engineering-2026)）。实施前要按 [Delta 版本矩阵](https://docs.delta.io/releases/) 选定与 Spark 4.1.0 匹配的最新版本，并固定 JAR 的校验和。

| # | 实验 | 验证 |
|---|---|---|
| 4.1 | 创建 Delta 表（路径方式，`s3a://spark-data/delta/...`） | 用 Silo Console 或 `silo-export.sh` 查看 `_delta_log/*.json` 和 Parquet 文件 |
| 4.2 | ACID 和事务日志 | 逐条解读每个 commit 中的 add 和 remove action |
| 4.3 | Schema Enforcement / Evolution | 写入不兼容的 schema 时被拒绝；使用 `mergeSchema` |
| 4.4 | MERGE / UPDATE / DELETE | 观察文件重写；开启 Deletion Vectors 后的变化 |
| 4.5 | Time Travel、VACUUM、OPTIMIZE | `versionAsOf`；VACUUM 之后旧版本不可读 |
| 4.6 | Optimistic Concurrency | 两个 Session 并发写入，观察冲突异常 |
| 4.7 | 对象存储上的多写入方 | 需核实：Delta 在 S3 上多集群写入依赖 put-if-absent（或 DynamoDB LogStore），要先确认 Silo 是否支持条件写入 |

### 14.5 Phase 5 — Catalog / Metadata

| # | 实验 | 现在可做 | 说明 |
|---|---|---|---|
| 5.1 | Spark 内置 Hive 支持（内嵌 Derby） | 是 | 镜像中已有 `spark-hive_2.13-4.1.0`、`hive-metastore-2.3.10` 和 Derby。`enableHiveSupport()` 加上存放在 PVC 上的 Derby，用于学习 Managed 与 External 表、`LOCATION 's3a://…'`、`DESCRIBE EXTENDED` |
| 5.2 | 独立 Hive Metastore 服务（PostgreSQL 存储） | 需核实 | 要核实 Hive 镜像的 arm64 支持，以及 Spark 4.1 内置的 2.3.10 客户端与 HMS 版本的兼容性（`spark.sql.hive.metastore.version` 和 jars）。这一步对应过去经验中"Hive Metastore 部署方式"这个未知项，但本实验结果不能反向证明 Yitu 当时的实现 |
| 5.3 | Unity Catalog OSS | 需核实 | 开源版本（Apache 2.0），每个 Spark 小版本对应一个 connector `unitycatalog-spark_<spark>_2.13`（[UC 文档](https://unitycatalog.io/docs/how-to/configure-spark/)）。需要确认有 Spark 4.1 的构件，以及它与所选 Delta 版本的对应关系。社区反馈 Spark 4.2 上表访问有问题（[示例仓库](https://github.com/arindamchoudhury/spark-delta-unitycatalog)），我们使用 4.1，仍需实测 |
| 5.4 | AWS Glue Data Catalog | 否 | 需要 AWS 账号和凭证，会产生费用，且无法在本地运行。建议只做概念对比；如果要实测，需要你提供账号并确认费用 |
| 5.5 | 对比文档 | 是 | HMS、Glue 和 UC 在对象模型、权限和治理范围上的差异，以事实为准，不简单等同 |

### 14.6 建议顺序与约束

1. Phase 2.1（History Server）最先做，后面所有实验都会用到它。
2. 然后做 Phase 2 的其余项，再按 3 → 4 → 5 的顺序推进，每个 Phase 都依赖前一个 Phase 的产出。
3. 每完成一个 Phase，先提交验证证据和问题清单，确认之后再继续（§10 第 15 条）。
4. 不引入 Airflow、Kafka 等范围外组件。Spark Operator 和 Spark Connect 都标为可选。

---

## 15. 多用户、资源归属与管理视图（方案，待批准，2026-11-22）

### 15.1 参考平台的做法

| 平台 | 管理员如何查看工作负载 | 如何归属到用户 | 来源 |
|---|---|---|---|
| Cloudera AI（CML） | Site Administration：概要统计、按团队和用户统计的 CPU、内存、运行数量时间序列，Sessions 列表可导出 CSV | 每个用户的工作负载运行在**各自的 Kubernetes namespace** 中；Sessions、Jobs 等元数据保存在内部 PostgreSQL（Pod 加 PV）；项目文件在 NFS 上 | [Monitoring](https://docs.cloudera.com/machine-learning/cloud/site-administration/topics/ml-monitoring.html)、[Architecture](https://docs.cloudera.com/machine-learning/cloud/architecture-overview/topics/ml-architecture-overview-cml.html)、[Spark on K8s](https://docs.cloudera.com/machine-learning/cloud/architecture-overview/topics/ml-architecture-overview-spark-on-kubernetes.html) |
| Databricks | Compute 页面列出集群和创建者；Usage 和 Cost 页面；`system.compute.*`、`system.billing.usage` 系统表 | 自动为计算资源打 `Creator`、`ClusterId`、`JobId` 等默认 tag，并传播到云主机和用量记录；可以用 compute policy 强制要求 tag | [Usage tags](https://docs.databricks.com/aws/en/admin/account-settings/usage-detail-tags)、[Compute system tables](https://docs.databricks.com/gcp/en/admin/system-tables/compute) |

两者有一个共同点：管理视图建在**平台自己的元数据**（数据库、tag 或 label）之上，而不是让管理员直接浏览 Pod。Databricks 的用户看到的是集群和作业，不是 Pod。

### 15.2 Headlamp 的定位

Headlamp 是 Kubernetes SIG UI 的项目，也是已归档的 Kubernetes Dashboard 的官方推荐替代（[Kubernetes Blog](https://kubernetes.io/blog/2026/06/01/dashboard-to-headlamp/)）。作为**通用 Kubernetes 运维 UI**，它是当前的最佳选择：可以查看 Pod、日志、Events、YAML，可以按 label 过滤。但它不知道"用户"和"Spark 应用"这些业务概念，只能显示 Pod 上已有的 label。它解决"看见 Pod"的问题，不解决"归属"的问题。归属要靠 §15.4 的 label。

### 15.3 JupyterHub 用户管理评估（不依赖外部身份源）

| Authenticator（hub 镜像中已有） | 自行注册 | 管理员审批 | 独立密码 | 结论 |
|---|---|---|---|---|
| DummyAuthenticator（当前） | 否 | 否 | 否，共享密码 | 不满足 |
| SharedPasswordAuthenticator（JupyterHub 5 内置） | 否 | 否 | 否 | 不满足 |
| FirstUseAuthenticator 1.1.1 | 首次登录时设置密码 | 否 | 是 | 缺少审批 |
| TmpAuthenticator | 匿名 | 否 | 否 | 不满足 |
| **NativeAuthenticator 1.3.0** | 是，`/hub/signup` | 是，`open_signup=False` 时管理员在 `/hub/authorize` 批准 | 是，密码以 bcrypt 哈希存放在 Hub 数据库；支持修改密码、最小长度、常见密码检查、登录失败锁定 | **满足全部要求** |

JupyterHub 本身负责：`/hub/admin` 管理用户（管理员标记、启停或删除服务器），KubeSpawner 为每个用户创建 `jupyter-<user>` Pod 和 `claim-<user>` PVC，并给 Pod 打上 `hub.jupyter.org/username=<user>` label（已核实）。

### 15.4 Spark 资源归属（已做原型验证）

| 层次 | 做法 | 状态 |
|---|---|---|
| Notebook Pod | KubeSpawner 自动打 `hub.jupyter.org/username` | 已有 |
| Driver 和 Executor Pod | `spark.kubernetes.driver.label.spark-lab/user=<user>` 和 `spark.kubernetes.executor.label.spark-lab/user=<user>` | 原型已验证：Cluster Mode 下 Driver 和 Executor 都带上了 `spark-lab/user` label |
| Spark 应用 | 环境变量 `SPARK_USER=<user>`，Spark UI 和 REST API 显示 `sparkUser` | 原型已验证：`sparkUser: alice`；Driver 和 Executor 的环境变量中都有 `SPARK_USER=alice`。目前所有容器都以 `jovyan` 运行，不设置的话每个应用都显示 `jovyan` |
| 自动注入 | Notebook 启动时为该用户生成 spark-defaults（带上述 label），并设置 `SPARK_USER` 环境变量。用户名来自 KubeSpawner 的 `{username}` 模板 | 待实施 |
| 防伪造 | 上面的 label 是提交端自己声明的，用户可以覆盖。真正可信的归属需要每个用户一个 ServiceAccount，再配合 admission policy 校验或改写 label。CML 用 per-user namespace 达到同样效果 | 待评估（第二步） |

多用户还需要修改的单用户假设：Client Mode 的 headless Service 只能对应一个 Driver（改为每个用户一个 Service，或使用 Pod IP 作为 `spark.driver.host`）；`/spark-ui/` 只指向一个 Notebook；Silo 使用共享的 root 密钥（改为每个用户一个 Silo 用户和对应策略）。

### 15.5 作业状态的维护

| 信息 | CML 的做法 | 本实验的现状 | 建议 |
|---|---|---|---|
| 用户和会话 | PostgreSQL | Hub 数据库（SQLite，PVC `hub-db-dir`）：用户、服务器、最近活动时间；NativeAuthenticator 的账号也在这里 | 多用户阶段把 Hub 数据库换成 PostgreSQL（Z2JH 支持 `hub.db.type: postgres`） |
| 运行中的工作负载 | Kubernetes | Kubernetes：Pod 和 label | 管理视图直接从 Kubernetes API 按 label 读取 |
| 作业历史和状态 | PostgreSQL | 没有持久记录：Completed 状态的 Driver Pod 会被清理，Events 约 1 小时后过期 | Spark History Server：event log 写入 `s3a://spark-logs/`，按应用保存状态、耗时、Executor 和 `sparkUser`（Phase 2.1） |
| 作业定义和调度 | PostgreSQL 中的 Jobs | 无 | 可选：Spark Operator 的 `SparkApplication`，状态记录在 CR 的 status（etcd）中 |
| 资源用量 | Usage 时间序列 | 无 | 后续可选：kube-state-metrics 加 Prometheus，按 `spark-lab/user` label 聚合 |
| 用户文件 | NFS | 每个用户一个 PVC（local-path） | 单节点环境够用 |

### 15.6 建议的实施顺序

1. NativeAuthenticator：注册、审批，`spark` 设为管理员。
2. 用户身份自动注入 Spark：label 加 `SPARK_USER`，并解决上面列出的单用户假设。
3. Spark History Server（Phase 2.1）。
4. 管理视图：Headlamp（只读），加上按用户汇总的 Portal 页面（Notebook、Driver、Executor）。
5. 防伪造的归属（per-user ServiceAccount 或 namespace）和资源用量监控，单独评估。


# task 2026-11-19
关于你的问题：
1. Mac Studio M5 Max / 64GB 很适合作为主实验机；不代表已验证可运行，因为磁盘、容器运行时、Minikube driver 及预留给 macOS 的内存尚未检查。M5 MacBook Pro 未给出内存和磁盘，暂不能判断是否足以运行完整 Client/Cluster/Silo 组合。
2. Kubernetes Client Mode 的意思是：Notebook Pod 中的 Jupyter kernel 进程本身就是 Spark Driver；Driver 通过 Kubernetes API 创建 Executor Pods。它要求 Executor 能通过稳定 DNS 和固定端口回连 Driver，因此新增了：
   - Notebook 固定标签；
   - headless Service；
   - Driver RPC 7078、BlockManager 7079；
   - 最小权限的 spark-client ServiceAccount / Role。
     Spark 官方明确支持该模式，并建议 Pod 内 Driver 使用 headless Service 和 spark.driver.host/`spark.driver.port`。官方文档
3. Cluster Mode 的意思是：Notebook 只提交 spark-submit；Kubernetes 新建独立的 Driver Pod，Driver 再创建 Executor Pods。Notebook Pod 既不是 Driver 也不是 Executor。它更接近批处理、声明式提交和 Spark Operator 的运行方式。
4. Spark on Kubernetes 是主流且被 Apache Spark 官方支持，但不是所有个人或 startup 的默认选择。已有 Kubernetes 平台、需要统一调度/镜像治理的团队常用；想减少运维成本时，Databricks、EMR、Dataproc、serverless Spark 往往更合适。Spark Operator 的主流使用方式也以 Cluster Mode 为主。Spark 官方文档 Spark Operator 文档
5. Phase 1 的难点不在 YAML 数量，而在这些 YAML 是否能真正让不同镜像、不同 Pod、RBAC、Driver 网络、S3A 依赖、PVC 与凭证一起工作。现在的模板把基础对象建立起来，但所有镜像 digest、版本、StorageClass、资源额度仍被 REPLACE_* 明确阻断，避免未经验证就部署。
6. Notebook Pod 内 pip install 只改变 Notebook/Driver 环境，不会自动传播给 Executor。README 已确定推荐顺序：受控 Spark runtime image 为基线；`--py-files` 用于小型纯 Python 项目代码；必要时再评估 Python 环境打包。Cluster Mode 的 Driver 和 Executor 应使用一致 runtime image。
7. CML 不能泛化为“每个 Notebook 一定关联独立 Driver Pod”。其具体拓扑取决于产品版本、runtime 和会话配置；官方资料能证明其使用容器化 Spark，并存在 Driver/Executor Pod 概念，但不能据此反推所有 Notebook 都有单独 Driver Pod。Cloudera AI Spark on Kubernetes
8. 登录采用个人本机学习用的 DummyAuthenticator，并在 README 明确标为不可暴露到公网；Zero-to-JupyterHub 官方也将其定位为测试用途。JupyterHub 认证文档
Silo 保持首选。模板使用 Silo 的集群内 S3 Endpoint，并要求先手动确认版本、镜像 digest、许可证和凭证；Silo 当前是 PGSTY 维护的 S3-compatible MinIO 分支。Silo 项目

下一步需要你的批准后，我才会开始“实施前环境核验”：只检查 Mac Studio 的 Docker/Minikube/Helm/kubectl 状态、CPU 架构、内存和磁盘，不下载或部署任何内容。

# task 2026-11-22
用户提出的改进及处理结果：
1. 一键启停：`startcluster.sh` / `stopcluster.sh`；Traefik Ingress 在 8080 端口对外提供服务，不再使用 port-forward，局域网可访问（§13.2）。
2. 统一入口 `portal/index.html`，包括 Spark UI（`/spark-ui/`、`/spark-driver/`）和 Silo Console（`/silo/`）。
3. Silo 的 Web 界面、存储挂载链路，以及"能否在 Mac 上看到文件"：README §4。新增 `scripts/silo-export.sh`。
4. `wordcount_local` 和 `wordcount_executor` 示例 Notebook 通过镜像和 init container 挂载到 `work/examples/`（README §3）。
5. Phase 2–5 评估与计划：§14。
6. Phase 1 验收状态：§7。还有一项未完成：GitHub Push，需要用户提供凭证。
