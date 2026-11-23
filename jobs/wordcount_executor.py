"""Kubernetes Client Mode WordCount (multi-user lab).

The Python process running this file (a Notebook kernel or `python3` in the
Notebook terminal) is the Spark Driver. The Driver asks the Kubernetes API to
create Executor Pods in YOUR namespace user-<name>, using the ServiceAccount
spark-runner mounted in your Notebook Pod. Executors connect back to the Driver
at this Pod's IP on random ports, so several Drivers in one Pod do not collide.

Run from the Notebook terminal:
    python3 ~/work/examples/jobs/wordcount_executor.py
"""
import os
import socket

from pyspark.sql import SparkSession

NAMESPACE = os.environ["SPARK_LAB_NAMESPACE"]          # user-<name>
DRIVER_HOST = os.environ["SPARK_LAB_DRIVER_HOST"]      # this Pod's IP
RUNTIME_IMAGE = os.environ.get("SPARK_LAB_RUNTIME_IMAGE", "spark-lab-runtime:0.3.0")
EXECUTORS = os.environ.get("WORDCOUNT_EXECUTORS", "2")


def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("WordCount-K8s-Client")
        .master("k8s://https://kubernetes.default.svc:443")
        .config("spark.submit.deployMode", "client")
        .config("spark.kubernetes.namespace", NAMESPACE)
        # Executors reach the Driver at the Pod IP; ports are random.
        .config("spark.driver.host", DRIVER_HOST)
        .config("spark.driver.bindAddress", "0.0.0.0")
        # Owner reference: Executor Pods are garbage-collected with this Pod.
        .config("spark.kubernetes.driver.pod.name", socket.gethostname())
        .config("spark.kubernetes.container.image", RUNTIME_IMAGE)
        .config("spark.kubernetes.container.image.pullPolicy", "Never")
        .config("spark.executor.instances", EXECUTORS)
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.kubernetes.executor.request.cores", "500m")
        .getOrCreate()
    )


def main() -> None:
    spark = build_session()
    sc = spark.sparkContext
    print(f"Driver Pod: {socket.gethostname()}  ip={DRIVER_HOST}  pid={os.getpid()}")
    print(f"Namespace: {NAMESPACE}  master: {sc.master}  "
          f"deployMode: {sc.getConf().get('spark.submit.deployMode')}")

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
