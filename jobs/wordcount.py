"""Phase 1 WordCount for spark-submit Kubernetes cluster mode.

It optionally reads from SILO_INPUT and writes to SILO_OUTPUT (both s3a://
URLs). Without those variables, it uses a small in-memory source so that the
Driver and Executor lifecycle can be observed independently of S3A.

WORDCOUNT_HOLD_SECONDS (optional) keeps the Driver alive after the job so its
Spark UI can be opened at /spark-driver/ on the lab portal.

The script is baked into the runtime image, so a Cluster-Mode Driver Pod can run
it as local:///opt/spark-lab/examples/jobs/wordcount.py
"""

import os
import time
from pyspark.sql import SparkSession


def main() -> None:
    builder = SparkSession.builder.appName("WordCount-Kubernetes-Cluster")

    endpoint = os.environ.get("SILO_ENDPOINT")
    access_key = os.environ.get("SILO_ACCESS_KEY")
    secret_key = os.environ.get("SILO_SECRET_KEY")
    if endpoint and access_key and secret_key:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )

    spark = builder.getOrCreate()
    input_path = os.environ.get("SILO_INPUT")
    output_path = os.environ.get("SILO_OUTPUT")

    if input_path:
        lines = spark.read.text(input_path).rdd.map(lambda row: row.value)
    else:
        lines = spark.sparkContext.parallelize(
            ["spark kubernetes", "spark notebook", "pyspark spark"], 2
        )

    counts = (
        lines.flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda left, right: left + right)
    )

    if output_path:
        counts.toDF(["word", "count"]).write.mode("overwrite").json(output_path)
    else:
        print(sorted(counts.collect()))

    hold = int(os.environ.get("WORDCOUNT_HOLD_SECONDS", "0"))
    if hold > 0:
        print(f"Holding the Driver for {hold}s; Spark UI: /spark-driver/")
        time.sleep(hold)
    spark.stop()


if __name__ == "__main__":
    main()
