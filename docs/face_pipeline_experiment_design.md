# 实验设计：人脸抓拍数据的分布式摄入、识别与按人聚合

**日期：** 2026-11-27
**状态：** 已记录，**暂不实施**（2026-11-27 用户决定）。重新启动前需用户确认规模（默认 100 万条抓拍、2 万人员库）和范围（建议先做 S1–S5，C++ 版与流式为可选）
**目的：** 在本实验平台上，用一个比 WordCount 更接近依图业务形态的流水线，亲手验证"多模态数据摄入 → 在 Executor 上调用原生代码做特征比对 → 按人脸 ID 聚合 → 分层落表 → 对账"这条链路的运行机制。

---

## 1. 边界

- **只用合成数据。** 不使用任何真实人脸图像、真实特征或个人数据。"人脸"是随机生成的身份，"特征"是随机向量，"图片"是随机字节。
- **验证的是机制，不是历史。** 实验结果说明 Spark 在 Kubernetes 上"可以怎样做、实际怎样运行"，**不能**反向证明依图当时采用了同样的实现（`solution_design.md` §3.2、§10 第 14 条）。
- **不引入范围外组件。** 不部署 Kafka、Airflow；流式部分用 Spark 自带的文件源。
- 运行在普通用户（如 `alice`）的命名空间内，受 8Gi 配额约束；不修改平台权限。

## 2. 与三份材料的对应

| 材料 | 侧重点 | 实验中对应的部分 |
|---|---|---|
| 依图经历（原始简历一句话，`solution_design.md` §3） | 高吞吐摄入、PySpark + C++ SDK、分区 Parquet、HDFS/MinIO、Spark on K8s | E1 摄入与小文件合并、E2 在 Executor 上调用原生库比对特征、分区落表、Executor 资源与 OOM |
| Lazada 简历 | 分层数仓（ODS / CDM / ADS）、指标与数据模型、海量下游报表 | 三层建模：原始抓拍 → 识别后明细 → 按人 / 按摄像头的汇总表 |
| 目标 JD（Databricks Lakehouse、监管报送） | Delta Lake、Spark SQL、数据质量、对账、审计证据 | E5 层间对账与准确率报告；后续升级为 Delta（MERGE 更新人员画像） |

## 3. 业务场景与合成数据

一个城市有 N 个摄像头，持续产生人脸抓拍。每条抓拍先经过特征提取得到向量，再和"已登记人员库"比对，得到人脸 ID（或判为陌生人），最后按人汇总轨迹和出现次数。

| 数据集 | 字段 | 默认规模 | 生成方式 |
|---|---|---|---|
| `cameras`（维表） | camera_id, district, lat, lon | 2,000 | 一次生成 |
| `gallery`（人员库） | person_id, embedding[128]（单位向量） | 20,000 人 | 一次生成，约 10 MB |
| `captures`（抓拍明细） | event_id, camera_id, ts, quality, bbox, embedding[128], thumb_key；**隐藏真值** true_person_id | 100 万条（参数 `SCALE` 可调） | 分布式生成：每个 task 负责一个摄像头分片和时间段 |
| `thumbs`（小图） | 每条抓拍一个 5–20 KB 的随机字节对象 | 与抓拍数相同（默认只生成 5% 以控制体量） | 刻意制造海量小文件 |

为了让聚合有真实的工程问题，生成器加入：

- **热点身份（数据倾斜）：** 出现次数服从 Zipf 分布，少数人占大量抓拍，聚合时会出现倾斜的 task。
- **陌生人：** 约 20% 的抓拍来自人员库以外的人，应被判为 unknown。
- **噪声：** 同一人的抓拍向量 = 登记向量 + 高斯噪声，噪声随 quality 增大而减小，所以低质量抓拍可能误识别。
- **重复与脏数据：** 少量重复 event_id、时间戳越界、空向量，用来练习清洗和对账。

因为生成器知道每条抓拍的真实身份，所以每一层的结果都可以和真值核对。

## 4. 流水线（各层存放在 `s3a://spark-data/users/<user>/face-lab/`）

```text
E0 生成     captures_raw/ (JSON Lines，按 dt/hour 分目录，大量小文件) + thumbs/ (小对象)
             │
E1 ODS      ods_captures/   Parquet，按 dt/hour 分区；去重、清洗；小文件合并
             │
E2 CDM      cdm_recognized/ 每条抓拍 + face_id（或 unknown）+ 相似度；在 Executor 上做向量比对
             │
E3 ADS      ads_person_daily/  按 face_id × 天：出现次数、去重摄像头数、首次/末次出现、主要出没区域、轨迹
            ads_camera_hourly/ 按摄像头 × 小时：抓拍数、识别率、陌生人比例
             │
E5 对账     recon_report/   层间行数、识别准确率、汇总与明细一致性
```

### E0 分布式生成

`sc.parallelize(range(num_shards), num_shards).mapPartitions(gen)`：每个 task 用自己的随机种子生成一批抓拍并直接写对象存储。观察点：生成任务本身就分布在多个 Executor 上，task 的主机名就是 Executor Pod 名。

### E1 摄入：小文件合并与分区

- 读入几万个小 JSON 文件，对比 task 数和文件数的关系、S3A LIST 的耗时。用 Silo 自带的 `mcli admin trace` 可以看到 S3A 实际发出的 LIST、HEAD、GET 请求。
- 按 `dt/hour` 分区写 Parquet，对比 `repartition`、`coalesce`、`maxRecordsPerFile` 对输出文件数的影响。
- `binaryFile` 数据源读取小图对象，统计数量和大小。这一步只演示多模态摄入，不解码图片。

### E2 识别：在 Executor 上调用原生代码比对特征（最接近依图的部分）

- 用 `mapInPandas` 按批处理抓拍：每个 Python worker **只加载一次**人员库（通过 `SparkFiles` 或从 S3A 读取，常驻内存），对每批向量做矩阵乘法求余弦相似度并取 top-1，低于阈值判为 unknown。
- 两个实现，用于对比：
  1. **numpy（BLAS）版：** numpy 底层的 BLAS 就是原生代码，已经体现"Python 调原生库"的模式。
  2. **C++ "SDK" 版（可选）：** 用 g++ 编译一个很小的共享库 `libfacematch.so`（暴力 top-1 搜索），Python 通过 `ctypes` 调用，镜像中已有 g++。这一版用来实际验证调用链：PySpark 的 Python worker 通过本地 socket 和 Executor JVM 交换 Arrow 批次，C++ 代码运行在 Python worker 进程里，**不经过 JNI**。只有从 Scala/Java UDF 调用原生代码时才用 JNI。
- 内存规划本身就是实验：批大小（`spark.sql.execution.arrow.maxRecordsPerBatch`）× 人员库大小决定相似度矩阵的内存。故意调大批大小，观察 Python worker 内存超出 Executor 内存限制后 Pod 被 OOMKilled，以及 Driver 如何重建 Executor。然后用 `spark.executor.pyspark.memory` 和 `memoryOverhead` 修正。
- 依赖分发：numpy 和 pyarrow 放进镜像（基线）；C++ 库对比两种分发方式，放进镜像和用 `--files` 分发，延续 Phase 1 §13.6 Q5 的结论。

### E3 聚合：按人脸 ID

- `groupBy(face_id, dt)` 计算出现次数、`countDistinct(camera_id)`、`min/max(ts)`、出现最多的区域；轨迹用窗口函数按时间排序后取摄像头序列。
- 和维表 `cameras` 关联：观察 broadcast join 和 shuffle join（调整 `spark.sql.autoBroadcastJoinThreshold`）。
- **数据倾斜：** 热点 face_id 导致少数 task 特别慢。对比 AQE 开关（`spark.sql.adaptive.enabled`、`skewJoin`），以及手工加盐后两阶段聚合的效果。在 Spark UI 的 Stage 页面看各 task 的耗时和 shuffle 读量分布。
- 扩展性：Executor 数取 1、2、4，记录 E2 和 E3 的耗时，验证"计算是否真的分摊到了多个 Executor"。

### E4 流式（可选）：按人实时累计，RocksDB 状态存储

- Structured Streaming 的文件源监听 `captures_raw/` 的新文件，E0 持续追加，模拟抓拍不断落地。
- 按 `face_id` 做带 watermark 的窗口聚合（如每 10 分钟出现次数），状态存储用 Spark 自带的 **RocksDBStateStoreProvider**（Spark 4.1 镜像中已有 `rocksdbjni-9.8.4`，已核实）。checkpoint 放在 S3A。
- 观察：状态存储放在 Executor 本地磁盘上的 RocksDB 实例里，在 Spark UI 的 Structured Streaming 页面可以看到 state rows、memory 等指标；重启查询后从 checkpoint 恢复。
- 这里出现的 RocksDB 是 Spark 状态存储的实现，**不同于**"自研 KV 存储引擎"，两者不能混为一谈。

### E5 对账与数据质量（对应 JD）

| 检查 | 规则 |
|---|---|
| 层间行数 | raw − 重复 − 脏数据 = ODS = CDM |
| 识别准确率 | CDM 的 face_id 与隐藏真值比较：准确率、误识率、陌生人召回率，按 quality 分段 |
| 汇总一致性 | Σ ADS 出现次数 = CDM 中已识别的行数 |
| 输出 | 一份对账报告（Parquet + Notebook 中的表格），作为"审计证据"的样例 |

## 5. 实验变量与观察记录

| 变量 | 取值 | 记录 |
|---|---|---|
| Executor 数 | 1 / 2 / 4 | E2、E3 耗时；Spark UI Executors 页面 |
| 输入文件数 | 原始小文件 / 合并后 | E1 task 数、LIST 请求数（`mcli admin trace`） |
| Arrow 批大小 | 1k / 10k / 50k | Python worker 内存、是否 OOMKilled |
| AQE、加盐 | 开 / 关 | 倾斜 Stage 的最长 task 耗时 |
| broadcast 阈值 | 默认 / -1 | 物理计划中的 join 类型（`explain`） |
| 原生实现 | numpy / C++ ctypes | 吞吐，以及调用链的证据（进程、PID） |

## 6. 资源估算（单用户配额 4 CPU / 8Gi）

- 默认 100 万条抓拍：JSON 约 1.5 GB，Parquet 约 0.6 GB。Silo 所在虚拟机磁盘约有 400 GB 可用空间；local-path 不强制执行 PVC 的 5Gi 限制。
- 比对计算量：100 万 × 2 万 × 128 ≈ 2.6 × 10¹² 次乘加。2–4 个 Executor 上预计需要数十秒到几分钟，实测后再定默认规模。
- Executor 每个 1g（约 1.4Gi）。4 个 Executor 加 Notebook 约 7.5Gi，刚好在配额内；E2 需要额外的 Python 内存，所以默认用 2–3 个 Executor。

## 7. 需要的改动（批准后实施）

| 改动 | 说明 |
|---|---|
| 镜像增加 numpy、pandas、pyarrow | Pandas UDF / `mapInPandas` 必需，镜像中目前都没有（已核实）。版本按 Spark 4.1 的 PySpark 依赖要求选定后固定 |
| 可选：镜像中编译 `libfacematch.so` | 约 50 行 C++，`ctypes` 调用 |
| `jobs/face_lab/`：gen / ingest / recognize / aggregate / recon | 每一步都可用 `spark-submit` 以 Cluster Mode 运行，也可在 Notebook 中运行 |
| `notebooks/04_face_pipeline.ipynb` | 一步步运行并展示观察点 |
| 平台本身 | 不改 |

## 8. 分步与验收

| 步骤 | 验收 |
|---|---|
| S1 镜像依赖 + E0 生成 | 100 万条抓拍分布式写入 Silo；task 分布在多个 Executor 上 |
| S2 E1 摄入 | ODS 分区正确；小文件合并前后的文件数和耗时有对比 |
| S3 E2 识别（numpy） | 准确率符合噪声设定；人员库每个 Python worker 只加载一次（有日志证据）；完成一次 OOM 复现和修正 |
| S4 E3 聚合 | ADS 结果与真值一致；有 AQE / 加盐前后的倾斜对比 |
| S5 E5 对账 | 报告中所有检查通过 |
| S6（可选）C++ 版、E4 流式 | 调用链证据；RocksDB 状态存储指标与故障恢复 |

## 9. 与原有路线图的关系

本实验可以作为 `solution_design.md` §14 后续阶段的载体：

- 覆盖 Phase 2 的资源与 OOM（2.3）、依赖分发（2.6），以及用 History Server 查看历史作业；
- 覆盖 Phase 3 的分区、小文件、S3A 请求观察（3.1、3.3、3.4）；
- 自然引出 Phase 4：把 `ads_person_daily` 改成 Delta 表，用 MERGE 按天更新人员画像，用 Time Travel 查看历史版本；
- 为 Phase 5 提供表：用 Hive Metastore 登记 ODS / CDM / ADS 三层表。

## 10. 附：依图问答稿需要核对的地方（2026-11-27）

这些与实验无关，但做实验时可以顺带验证。

| 位置 | 原文 | 问题 |
|---|---|---|
| Q1 RocksDB 写入路径 | "MemTable → WAL → SSTable" | 顺序是先写 WAL，再写 MemTable，MemTable 写满后刷成 SSTable |
| Q2 混语言交互 | "高效的 JNI / 进程间管道设计" | PySpark 中 Python worker 与 Executor JVM 通过本地 socket 交换数据（Arrow 批）；从 Python 调用 C++ SDK 时，C++ 代码运行在 Python worker 进程里（ctypes / pybind11），不经过 JNI。JNI 只用于从 Scala/Java 调用原生代码。E2 的 C++ 版可以给出调用链证据 |
| 开场白、Q1 | "自研 / 深度定制 RocksDB 存储引擎""百亿级""Prefix Bloom Filter" | 原始简历（`solution_design.md` §3）没有这些内容。面试中只讲自己确实做过、能展开的部分 |
| E4 与 Q1 | — | E4 中的 RocksDB 是 Spark 自带的流式状态存储，不是"自研 KV 存储引擎"，两者不能混为一谈 |
| 开场白 | "人肉脸识别" | 应为"人脸识别" |

