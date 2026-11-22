下面整理成一份完整的技术讨论文档，分为两大部分：**Spark on Kubernetes Cluster Mode 的任务处理机制**，以及**Spark 与专用分布式计算程序的性能开销和适用场景比较**。不包含性能测试计划或测试要求，也会区分已经明确的技术机制与需要结合具体工作负载判断的性能结论。

# Spark on Kubernetes 的任务执行机制与分布式计算性能分析

## 1. 文档目的

本文整理 Spark on Kubernetes Cluster Mode 的任务提交、Driver 和 Executor 的职责、Python 程序的分发方式，以及 Spark 与专用分布式计算程序之间的性能和架构取舍。

核心讨论两个问题：

1. 在 Cluster Mode 下，用户提交的 Python 程序如何进入 Kubernetes 集群，并由谁执行？
2. Spark 的通用分布式计算框架会产生哪些额外开销？与针对固定计算任务自行开发的 Python Worker 或 MPI 程序相比，Spark 的优势和局限是什么？

本文不假设 Spark 在所有任务中都更快，也不假设自研程序一定更快。具体性能取决于计算模式、数据分布、通信量、任务规模和实现方式。

---

# 第一部分：Spark on Kubernetes 的任务执行机制

## 2. Spark on Kubernetes 的基本架构

Spark on Kubernetes 使用 Kubernetes 作为资源管理和容器调度平台。

它与 Spark Standalone、YARN 有明显区别：

| 计算架构                | 资源管理和调度系统                         |
| ------------------- | --------------------------------- |
| Spark Standalone    | Spark Master / Worker             |
| Hadoop YARN         | ResourceManager / NodeManager     |
| Spark on Kubernetes | Kubernetes API Server / Scheduler |

Spark on Kubernetes 不需要额外部署 Spark Master。

提交参数：

```
--master k8s://https://kubernetes.default.svc
```

这里的 `k8s://` 指向 Kubernetes API Server，而不是 Spark Master。

整体架构：

```
                 用户
                  |
                  v
           Jupyter Notebook
                  |
                  | spark-submit
                  v
          Kubernetes API Server
                  |
                  v
             Driver Pod
                  |
          +-------+-------+
          |               |
          v               v
     Executor Pod 1  Executor Pod 2
          |               |
          v               v
       执行 Tasks      执行 Tasks
          |               |
          +-------+-------+
                  |
                  v
             返回结果
```

Kubernetes 负责 Pod 的创建、资源分配、节点调度和生命周期管理。

Spark 负责 Application 内部的 Job、Stage、Task、Executor 和数据计算协调。

两者的职责不能混为一谈。

## 3. Cluster Mode 下任务是如何提交和执行的？

以用户在 Jupyter Notebook 中提交 Python 文件为例：

```
spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --conf spark.kubernetes.namespace=spark-alice \
  --conf spark.kubernetes.container.image=my-spark:3.5 \
  local:///opt/spark/work-dir/word_count.py
```

实际执行过程如下。

### 3.1 提交阶段

用户在 Notebook Pod 中执行 `spark-submit`。

`spark-submit` 负责构造提交请求，并通过 Kubernetes API 提交 Spark Application。

在 Cluster Mode 下，Spark 创建一个独立的 Driver Pod。

Notebook Pod 并不承担这个 Application 的 Driver 工作。

### 3.2 Driver 启动阶段

Kubernetes Scheduler 选择合适的节点，启动 Driver Pod。

Driver Pod 中启动 Spark Driver JVM。

对于 PySpark 程序，Driver JVM 还会启动 Python Driver 进程执行用户提交的 `.py` 文件。

需要区分：

- Driver JVM：负责 Spark Application 的核心调度和协调。
- Python Driver：执行用户编写的 Python 应用程序。
- Executor JVM：执行 Spark 分配的计算任务。
- Python Worker：在需要执行 Python UDF 等操作时，由 Executor JVM 启动。

### 3.3 Executor 创建阶段

Driver 通过 Kubernetes API 创建 Executor Pods。

Executor 启动后向 Driver 注册。

Driver 根据数据分区和执行计划，将 Task 分配给 Executor。

Executor 执行实际计算，并将状态和结果反馈给 Driver。

### 3.4 Application 结束阶段

Application 执行完成后：

- Driver 结束运行。
- Executor Pods 终止。
- Kubernetes 回收相关计算资源。
- Spark Event Log 可以写入持久化存储，供 Spark History Server 查看历史执行信息。

需要注意：Driver Pod 是 Application 的控制进程，不是所有 Spark Application 共用的常驻计算服务。

## 4. 每个任务是否都要创建一个 Driver？

这里需要区分 Application、Job、Stage 和 Task。

| 概念                | 含义                   | 是否通常需要独立 Driver |
| ----------------- | -------------------- | --------------- |
| Spark Application | 一个独立 Spark 应用程序      | 是               |
| Spark Job         | 一次 Action 触发的计算任务    | 否               |
| Stage             | Job 内部的执行阶段          | 否               |
| Task              | 分配给 Executor 的具体计算单元 | 否               |

例如：

```
df = spark.read.parquet("s3a://data/orders")

df.groupBy("city").count().show()

df.groupBy("product").sum("amount").show()
```

两次 `show()` 会触发两个 Spark Job，但它们可以由同一个 Spark Application、同一个 Driver 和同一组 Executor 管理。

因此：

**不是每个 Job 或 Task 都创建一个 Driver，而是标准 Cluster Mode 通常为每个独立 Spark Application 创建一个 Driver Pod。**

这种模式有独立生命周期和资源隔离方面的好处，但对于大量短小任务，Driver 启动和 Pod 调度开销可能比较明显。

### 4.1 Driver 是否可以长期运行？

需要区分普通 Spark Application Driver 和远程交互式计算服务。

普通 Spark Application 通常拥有独立 Driver。

但是，可以通过 Spark Connect 等远程交互架构，让 Notebook 连接到长期运行的服务端，而不是每次在 Notebook 中启动一个新的 Driver。

Livy 也提供远程 Spark Session 和 Batch 管理能力。

这些架构能够改善交互式使用体验，但不意味着所有用户的独立 Application 可以简单地共享一个普通 Driver。

## 5. Driver 与 Executor 的职责

### 5.1 Driver：控制和调度

Driver 主要负责：

- 创建 SparkContext 和 SparkSession。
- 构建执行计划。
- 将 Job 拆分为 Stage。
- 将 Stage 拆分为 Task。
- 调度 Task 到 Executor。
- 管理 Executor 状态。
- 协调 Application 的运行和结束。
- 收集执行结果和运行状态。

Driver 是 Spark Application 的控制中心。

### 5.2 Executor：执行计算

Executor 主要负责：

- 执行 Task。
- 处理数据分区。
- 执行计算和数据转换。
- 缓存数据。
- 执行 Shuffle 的数据读写。
- 返回 Task 执行状态和结果。

对于分布式数据处理，Executor 通常承担主要的数据计算资源消耗。

但 Driver 不是无用的附属组件。没有 Driver，就没有 Spark Application 内部的任务协调和调度机制。

## 6. Python 文件最终被谁执行？

这是 Cluster Mode 中一个容易混淆的问题。

假设 Python 文件位于 Notebook Pod：

```
Notebook Pod
  /home/alice/word_count.py
```

这并不代表 Driver Pod 能自动访问这个文件。

因为 Notebook Pod 和 Driver Pod 是不同的容器，具有独立的文件系统。

因此，需要解决程序文件分发问题。

常见方式：

### 方式一：打包进容器镜像

构建 Spark 镜像时，将 Python 文件和依赖放入镜像。

例如：

```
/opt/spark/work-dir/word_count.py
```

提交时使用：

```
local:///opt/spark/work-dir/word_count.py
```

这里的 `local://` 表示文件已经存在于 Driver 容器的本地文件系统中。

优点：简单、稳定。

缺点：代码更新需要重新准备镜像，动态提交不方便。

### 方式二：使用共享 PVC

将共享 PVC 挂载到 Notebook Pod 和 Driver Pod。

Notebook 上传 Python 文件，Driver Pod 从共享目录读取。

优点：适合动态提交程序。

缺点：需要考虑存储访问、并发和生命周期管理。

### 方式三：使用 MinIO / S3 对象存储

Notebook 将程序和依赖上传到 MinIO。

例如：

```
s3://spark-code/alice/app-001/word_count.py
```

Driver Pod 通过对象存储读取程序。

需要注意：Spark 不同版本对 Python Application 文件和依赖资源的分发支持有具体要求，不能直接假设任意 S3 URI 都能作为 PySpark 主程序运行。

可以使用 Driver 启动包装程序，在 Driver 容器启动后下载 Python 文件，再执行用户程序。

### 方式四：通过 HTTP 或其他文件服务下载

Driver Pod 启动时，从文件服务下载任务代码。

适合需要统一控制代码版本、权限和依赖的场景。

### 6.1 对平台的建议

可以考虑如下流程：

```
用户
 |
 v
Jupyter Notebook
 |
 | 上传代码
 v
MinIO / Silo
 |
 | 提交任务
 v
Job Submission Service（可选）
 |
 | 创建 Spark Application
 v
Driver Pod
 |
 | 创建 Executor Pods
 v
Executor Pods
 |
 v
计算结果
```

其中，Job Submission Service 是可选的管理组件，不是 Spark Cluster Mode 必需的组件。

它可以承担用户身份验证、代码归档、提交权限检查、任务状态管理等工作。

---

# 第二部分：Spark 的性能开销与专用分布式计算比较

## 7. Spark 为什么会有额外开销？

Spark 的性能开销主要可以从四个方面理解。

### 7.1 Kubernetes Pod 和容器启动开销

Cluster Mode 通常涉及：

1. 创建 Driver Pod。
2. 调度 Driver Pod。
3. 启动 Driver JVM。
4. 创建 Executor Pods。
5. 调度 Executor Pods。
6. 启动 Executor JVM。
7. Executor 注册到 Driver。

这些步骤都需要时间和资源。

对于运行几秒钟的小任务，启动开销可能占总耗时的很大比例。

对于运行几十分钟或数小时的大任务，启动成本的相对占比通常会下降。

另外，同一个 Application 内可以连续执行多个 Job，并复用已经运行的 Executor，因此不能把每个 Job 都计算成一次完整的 Pod 启动过程。

### 7.2 Spark Shuffle 开销

Shuffle 是 Spark 分布式计算中一个重要的性能成本来源。

例如：

```
df.groupBy("user_id").count()
```

如果相同的 `user_id` 分散在不同节点，就需要把相关数据发送到对应的分区。

可能产生：

- 网络传输。
- 序列化和反序列化。
- 磁盘写入和读取。
- 内存管理。
- 数据排序和合并。

对于大规模数据，Shuffle 可能成为主要性能瓶颈。

但 Spark 并不是所有操作都会产生 Shuffle。

### 7.3 窄依赖与宽依赖

窄依赖：

```
df.filter(df.age > 18)
df.select("name", "age")
df.withColumn("age2", df.age + 1)
```

这些操作通常可以在原数据分区内执行，不需要全量跨节点重新分配数据。

宽依赖：

```
df.groupBy("user_id").count()
df.join(other_df, "user_id")
df.repartition(100)
```

这些操作可能需要 Shuffle。

因此，Spark 的通信开销取决于实际计算逻辑，而不是每个操作都必然产生大量网络传输。

### 7.4 通用调度和执行框架开销

Spark 需要维护：

- Application 生命周期。
- Job、Stage、Task 调度。
- Executor 状态。
- 数据分区。
- 任务重试。
- 数据缓存。
- 序列化机制。
- 执行计划。
- 运行时状态。

这些机制使 Spark 能够支持多种数据处理任务，但也会带来一定的执行开销。

专用程序可以针对固定计算场景简化这些机制。

---

## 8. 专用 Python 分布式程序的架构

假设有 4 台机器，每台机器负责一部分数据。

可以自行设计：

```
              Master
                |
       +--------+--------+
       |        |        |
       v        v        v
    Worker A Worker B Worker C
       |        |        |
       v        v        v
    数据分片A 数据分片B 数据分片C
       |        |        |
       +--------+--------+
                |
                v
             汇总结果
```

每个 Worker 负责固定的数据分片。

如果任务之间没有复杂的数据交换，只需要最后汇总结果，那么通信量可能非常小。

例如：

```
def process_partition(data):
    result = {}

    for item in data:
        key = item["category"]
        result[key] = result.get(key, 0) + item["value"]

    return result
```

各节点只需要把局部统计结果发送给 Master。

如果数据分片规则合理，就可以减少不必要的跨节点数据交换。

这种方式可能比通用 Spark 执行模型更加高效。

但自研程序需要自行解决：

- Worker 注册和发现。
- 任务分配。
- 失败检测。
- 任务重试。
- 数据分片和重新分配。
- 结果汇总。
- 数据一致性。
- 并发控制。
- 运行日志和监控。

如果任务非常简单、分片固定，这些工作可能很容易。

如果任务复杂、动态变化、节点经常发生故障，自研成本就会明显增加。

---

## 9. Spark 与专用分布式程序的性能和能力比较

| 比较维度            | Spark on Kubernetes     | 专用 Python Worker | Python + MPI   |
| --------------- | ----------------------- | ---------------- | -------------- |
| 初始启动            | 需要 Spark 和 Pod 启动流程     | 可以复用常驻 Worker    | 通常需要启动 MPI 进程  |
| 计算调度            | Spark 自动调度              | 自行实现             | MPI 程序自行控制     |
| 数据分片            | Spark 管理分区              | 自行设计             | 自行设计           |
| 数据交换            | Spark Shuffle 等机制       | 自行设计通信协议         | MPI 通信机制       |
| 任务重试            | 提供一定的任务级容错              | 需要自行实现           | 需要结合应用设计       |
| 动态资源管理          | 支持 Kubernetes 资源管理及动态分配 | 需要自行实现或集成        | 通常由 MPI 运行环境管理 |
| SQL / DataFrame | 原生支持                    | 需要自行开发           | 需要自行开发         |
| Join / GroupBy  | 有成熟实现和优化机制              | 需要自行实现           | 需要自行实现         |
| 运行监控            | Spark UI、History Server | 自行开发             | 自行开发或集成        |
| 算法针对性优化         | 受到 Spark 执行模型约束         | 可以高度定制           | 可以高度定制         |
| 开发和维护成本         | 通用框架承担大量工作              | 取决于自研范围          | 需要掌握并维护 MPI 程序 |

### 9.1 专用程序可能更快的情况

以下工作负载通常值得考虑专用分布式程序：

1. 数据分片规则长期固定。
2. 每个节点负责相互独立的数据。
3. 节点之间通信量很小。
4. 计算算法明确，不需要复杂动态调度。
5. 对执行延迟和资源利用率要求较高。
6. 可以接受自行维护分布式任务管理机制。

例如：

- 固定数据分片的统计分析。
- 独立图像或文件批处理。
- 固定规则的数值计算。
- 节点本地计算，最后合并结果。
- 具有明确通信模式的专用算法。

### 9.2 Spark 更有价值的情况

Spark 的价值主要体现在通用性和工程能力：

1. 数据规模和分布可能变化。
2. 数据处理逻辑复杂。
3. 需要大量 SQL、Join、GroupBy 和 ETL。
4. 需要统一的数据处理 API。
5. 需要任务级重试和运行管理。
6. 不希望每个业务团队都重新开发分布式执行框架。
7. 需要成熟的 SQL 优化、数据分区和缓存机制。

Spark 的核心价值并不是保证每个计算都达到最低延迟，而是通过统一的计算抽象降低分布式数据处理的开发和维护成本。

---

## 10. 已有研究中的 Spark 与 MPI 性能比较

Spark 与 MPI 的性能差距已经有学术研究进行过比较。

需要强调：不同论文采用的算法、Spark 版本、硬件环境和数据规模并不相同，论文中的倍数不能直接当成所有工作负载的通用结论。

### 10.1 CMU：Benchmarking Apache Spark with Machine Learning Applications

该研究比较了 Spark 与更直接的分布式通信实现。

研究观察到，在某些机器学习任务中，Spark 的数据交换和 Shuffle 机制会限制扩展效率。

研究报告中的一个矩阵分解案例显示，Spark 从 1 台机器扩展到 8 台机器，获得约 5.5 倍加速；继续增加资源并没有得到相应的线性加速。

这说明通用计算框架的通信机制可能成为特定算法的瓶颈。

### 10.2 IBM：Understanding and optimizing the performance of distributed machine learning applications on Apache Spark

该研究比较了 Spark 和 MPI 实现的机器学习应用。

研究报告指出，未经优化时，Spark 与 MPI 之间可能存在较大的性能差距；经过针对性优化后，差距可以显著缩小。

这说明性能差异不完全来自 Spark 本身，还受到数据交换、算法设计和执行配置影响。

### 10.3 Python MPI 与 Spark 的比较研究

2025 年的一项研究报告，在其测试的计算密集型任务中，Python + MPI 相对 Spark 取得了约 2～10 倍的性能优势。

这一数字仅代表论文所测试的任务和环境，不应该推广为 Python + MPI 在所有场景下都比 Spark 快 2～10 倍。

### 10.4 对研究结果的综合理解

这些研究共同反映出一个事实：

对于特定、可预测的计算任务，专用通信和计算实现可能明显优于通用 Spark 执行引擎。

但是，不能仅凭这些结果得出 MPI 或 Python Worker 在所有分布式任务中都优于 Spark。

Spark 的成熟数据处理能力、调度、容错和开发效率，也是整体技术成本的一部分。

---

# 第三部分：平台架构层面的建议

## 11. 不要把 Spark 与计算平台绑定为同一个概念

一个计算平台不一定只能有一个计算引擎。

平台基础设施和计算引擎应该尽量解耦。

例如：

```
                  用户
                   |
                   v
              JupyterHub
                   |
                   v
             Notebook Pod
                   |
                   v
          Job Submission / API
                   |
        +----------+----------+
        |          |          |
        v          v          v
      Spark     Python      MPI
      Engine    Workers     Engine
        |          |          |
        v          v          v
     Executors   Workers    MPI Nodes
        |          |          |
        +----------+----------+
                   |
                   v
              Silo / MinIO
```

其中：

- JupyterHub：用户管理和 Notebook 生命周期。
- Kubernetes：容器、资源和节点管理。
- Silo / MinIO：持久化对象存储。
- Job Submission Service：统一提交和管理任务。
- Spark：通用数据处理引擎。
- Python Worker：固定分片和专用计算。
- MPI：需要精细控制进程间通信的计算任务。

用户不必关心底层到底使用哪种引擎，只需要通过统一入口提交和管理任务。

## 12. 三种平台发展路线

### 路线 A：以 Spark 为主要计算引擎

所有主要计算任务都使用 Spark。

优点：

- 架构相对统一。
- 有成熟的 SQL 和 DataFrame 能力。
- 有成熟的任务调度和运行管理。
- 降低自研分布式计算框架的工作量。

局限：

- 对于非常简单的固定计算，可能存在不必要的框架开销。
- 用户需要接受 Spark 的执行模型。
- 短小任务可能受到启动和调度成本影响。

### 路线 B：Spark 与专用计算引擎并存

Spark 负责通用数据处理。

Python Worker 或 MPI 负责适合专用计算模型的任务。

优点：

- 可以根据任务选择计算引擎。
- 不需要所有计算都承担 Spark 的执行模型。
- 保留 Spark 的 SQL、ETL 和数据处理能力。

局限：

- 需要统一任务管理和资源管理。
- 多种引擎需要分别维护。
- 数据格式、权限和运行日志需要统一设计。

### 路线 C：完全自研分布式计算框架

自己开发 Master、Worker、任务调度、分片和通信机制。

优点：

- 可以针对固定业务高度优化。
- 可以减少通用计算框架的部分开销。
- 可以完全控制任务执行流程。

局限：

- 自研范围可能不断扩大。
- 故障恢复和资源管理需要自行实现。
- 长期维护成本需要认真考虑。

## 13. 综合结论

### 13.1 关于 Driver

标准 Spark on Kubernetes Cluster Mode 通常为每个独立 Application 创建一个 Driver Pod。

Driver 是 Application 的控制和调度进程，Executor 是主要的分布式计算执行进程。

不是每个 Job 或 Task 都需要新的 Driver。

交互式计算可以采用长期运行的远程服务架构，减少反复启动成本，但不能简单地将普通 Driver 作为所有独立 Application 的公共调度中心。

### 13.2 关于 Spark 的性能

Spark 确实存在额外开销，主要包括：

- Driver 和 Executor 的启动。
- Kubernetes Pod 创建与调度。
- JVM 初始化。
- 通用任务调度。
- Shuffle 网络传输。
- 序列化和反序列化。
- 数据缓存和内存管理。

对于固定分片、低通信量、计算逻辑简单的专用任务，自研 Python Worker 或 MPI 程序可能更高效。

对于复杂数据分析、SQL、Join、GroupBy、ETL 等通用数据处理任务，Spark 的成熟执行引擎和工程能力具有实际价值。

### 13.3 关于平台定位

建议将 Spark 视为平台提供的一种计算引擎，而不是整个平台唯一的计算方式。

JupyterHub、Kubernetes、Silo / MinIO 和统一任务管理服务，可以作为平台的公共基础设施。

Spark、Python Worker、MPI 等则作为可选择的计算后端。

最终的设计目标应该是：

**统一平台基础设施，支持多种计算引擎，根据工作负载选择适合的执行方式。**

这比强制所有任务使用 Spark 更具有架构弹性。