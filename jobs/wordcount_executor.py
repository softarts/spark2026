"""Phase 1 Kubernetes Client Mode WordCount.

The Python process running this file (a Notebook kernel or `python3` in the
Notebook terminal) is the Spark Driver. The Driver asks the Kubernetes API to
create Executor Pods, which connect back to it through the headless Service
spark-client-driver.spark-lab.svc (ports 7078/7079).

Run from the Notebook terminal:
    python3 ~/work/examples/jobs/wordcount_executor.py

Only one Client-Mode Driver can hold ports 7078/7079 at a time: stop any
Client-Mode SparkSession in a Notebook before running this script.
"""
import os
import socket

from pyspark.sql import SparkSession

NAMESPACE = os.environ.get("SPARK_LAB_NAMESPACE", "spark-lab")
DRIVER_HOST = os.environ.get("SPARK_LAB_DRIVER_SERVICE", "spark-client-driver.spark-lab.svc")
RUNTIME_IMAGE = os.environ.get("SPARK_LAB_RUNTIME_IMAGE", "spark-lab-runtime:0.3.0")


def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("WordCount-K8s-Client")
        .master("k8s://https://kubernetes.default.svc:443")
        .config("spark.submit.deployMode", "client")
        .config("spark.kubernetes.namespace", NAMESPACE)
        # Executors reach the Driver through the headless Service.
        .config("spark.driver.host", DRIVER_HOST)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        # Owner reference: Executor Pods are garbage-collected with this Pod.
        .config("spark.kubernetes.driver.pod.name", socket.gethostname())
        .config("spark.kubernetes.container.image", RUNTIME_IMAGE)
        .config("spark.kubernetes.container.image.pullPolicy", "Never")
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-client")
        .config("spark.executor.instances", "2")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.kubernetes.executor.request.cores", "500m")
        .getOrCreate()
    )


def main() -> None:
    spark = build_session()
    sc = spark.sparkContext
    print(f"Driver host (this Pod): {socket.gethostname()}  pid={os.getpid()}")
    print(f"Master: {sc.master}  deployMode: {sc.getConf().get('spark.submit.deployMode')}")

    lines = sc.parallelize(
        [
            "spark on kubernetes client mode",
            "interactive spark notebook with dynamic executors",
            "driver in notebook pod executors in their own pods",
            "spark executors report back to the driver",
        ],
        4,
    )
    counts = (
        lines.flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda left, right: left + right)
    )
    print(sorted(counts.collect()))

    # Evidence: which Pod ran each task? Executor hostnames are Executor Pod names.
    where = (
        sc.parallelize(range(8), 8)
        .map(lambda _: socket.gethostname())
        .distinct()
        .collect()
    )
    print(f"Tasks ran on: {sorted(where)}")

    spark.stop()
    print("SparkSession stopped; Executor Pods are being deleted.")


if __name__ == "__main__":
    main()
