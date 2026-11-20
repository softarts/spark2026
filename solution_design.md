# Spark Platform Learning Lab — Solution Design

**文档类型：** Solution Design / Implementation Brief  
**目标读者：** 后续负责实现本实验环境的 AI Agent 或工程师  
**当前范围：** Phase 1 — Interactive Spark Development Environment  
**文档状态：** 设计基线；具体版本和部署细节须在实现前验证  
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

1. Minikube
2. Kubernetes
3. JupyterHub
4. User Notebook Pod
5. Apache Spark / PySpark
6. Silo（作为优先评估的 S3-compatible Object Storage 候选）
7. External GitHub
8. Persistent Volume / PVC
9. Spark UI

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
| Minikube | 本地 Kubernetes 集群 |
| JupyterHub | 登录入口、用户服务器生命周期管理 |
| User Notebook Pod | 用户开发环境，保存 Notebook，安装 Python 依赖，运行 PySpark |
| Apache Spark | 分布式计算引擎 |
| Spark Driver | Spark 应用协调、计划和任务调度 |
| Spark Executors | 执行分配的 Tasks、缓存数据并向 Driver 汇报 |
| Silo | 提供 S3-compatible Object Storage API |
| Git | Notebook Pod 内的本地版本控制客户端 |
| GitHub | 外部远程代码仓库，不部署在 Minikube 内 |
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

无需在 Minikube 内部署 Git Server 或 GitHub Enterprise。

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

### User Experience

- [ ] Minikube Kubernetes Cluster 正常运行
- [ ] 用户可以登录 JupyterHub
- [ ] JupyterHub 能创建 User Notebook Pod
- [ ] Notebook 内容在 Pod 重启或重建后按预期持久化（PVC）
- [ ] Notebook Pod 可以访问外部 GitHub
- [ ] 可以 Clone、Commit、Push
- [ ] Notebook 中可以运行 PySpark

### Spark Local Mode

- [ ] 创建 SparkSession
- [ ] WordCount Local Mode 成功
- [ ] 可以访问 Spark UI
- [ ] 确认 Driver 进程位置
- [ ] 确认 Local Mode 的执行进程和线程所在位置

### Spark Kubernetes Client Mode

- [ ] WordCount Client Mode 成功
- [ ] 确认 Driver 位置
- [ ] 观察到 Kubernetes Executor Pods
- [ ] 能够将 Spark UI Executor 信息与 Kubernetes Pods 对应
- [ ] 观察 Executor 结束后的资源清理行为

### Spark Kubernetes Cluster Mode

- [ ] 独立 `wordcount.py` 可提交
- [ ] 确认 Kubernetes 创建独立 Driver Pod
- [ ] 观察 Executor Pods
- [ ] 确认应用结束后的 Pod 生命周期
- [ ] 理解 Notebook 与 Driver 的关系

### Object Storage

- [ ] Silo 正常运行
- [ ] Bucket 创建成功
- [ ] Spark Driver 可访问 Silo
- [ ] Spark Executors 可访问 Silo
- [ ] Spark S3A 读取成功
- [ ] Spark S3A 写入成功
- [ ] 输出对象可从 Silo 侧确认

### Final Documentation

- [ ] 输出最终部署架构图
- [ ] 输出 Local / Client / Cluster 三种模式的执行链路图
- [ ] 明确回答 Notebook 是否就是 Driver
- [ ] 明确回答 Driver 何时创建
- [ ] 明确回答 Driver 如何创建和管理 Executor
- [ ] 明确回答 Executor 何时销毁
- [ ] 明确回答 Python 依赖如何提供给 Executor
- [ ] 明确回答 Notebook Interactive 模式与 Batch Spark 模式的区别
- [ ] 记录实际组件版本和关键配置

---

## 8. Phase 1 Final Deliverables

Phase 1 的主要产出不是单纯的代码仓库，而是经实验验证的架构理解和运行机制文档。

必须交付：

1. **可运行的实验环境**
   - Minikube
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
   - Kubernetes / Minikube
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
9. GitHub 使用外部托管服务；Minikube 内只安装 Git 客户端。
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
- Minikube 节点资源是否足够同时运行 JupyterHub、Notebook、Spark Driver、多个 Executors 和 Silo？
- Silo 当前发布、维护和 S3 API 兼容性是否满足实验需求？
- Spark S3A Connector 对应的 Hadoop AWS 与 AWS SDK 版本组合是什么？
- Spark UI 通过何种方式从本地浏览器访问？
- Cluster Mode 的 Driver Pod 如何获得 Silo 凭证？
- Notebook Pod、Driver Pod、Executor Pods 的 Python 环境如何保持兼容？

所有问题应通过官方文档、版本矩阵和实际实验解决，并在实现文档中记录来源和结论。

---

## 12. Definition of Success

Phase 1 成功的定义不是“Minikube 里安装了几个组件”，而是：

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