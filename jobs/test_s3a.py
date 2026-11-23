"""Test Spark 4.1.0 S3A read/write with Silo in-cluster."""
import os
import sys
import time
from pyspark.sql import SparkSession

endpoint = os.environ.get("SILO_ENDPOINT", "http://silo.spark-lab.svc:9000")
access_key = os.environ.get("SILO_ACCESS_KEY")
secret_key = os.environ.get("SILO_SECRET_KEY")

if not access_key or not secret_key:
    print("ERROR: SILO_ACCESS_KEY or SILO_SECRET_KEY not set", file=sys.stderr)
    sys.exit(1)

print(f"Configuring Spark with endpoint: {endpoint}")

spark = (
    SparkSession.builder
    .appName("Test-S3A-Silo")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

ts = int(time.time())
bucket = "spark-data"
# Per-user prefix (a convention, not isolation): s3a://spark-data/users/<name>/.
# Deleting the user in JupyterHub also deletes this prefix.
user = os.environ.get("SPARK_USER", "unknown")
input_dir = f"s3a://{bucket}/users/{user}/test-s3a/run-{ts}/input"
output_dir = f"s3a://{bucket}/users/{user}/test-s3a/run-{ts}/output"

print(f"Writing sample dataset to {input_dir} using DataFrame overwrite...")
sample_rows = [
    ("spark kubernetes",),
    ("spark notebook",),
    ("pyspark spark silo object storage",),
    ("spark hadoop s3a aws sdk v2",)
]
df_in = spark.createDataFrame(sample_rows, ["line"])
df_in.write.mode("overwrite").text(input_dir)

print(f"Reading from S3A at {input_dir}...")
lines_rdd = spark.read.text(input_dir).rdd.map(lambda row: row.value)
counts = (
    lines_rdd.flatMap(lambda line: line.split())
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)
results = sorted(counts.collect())
print("WordCount Results:")
for word, count in results:
    print(f"  {word}: {count}")

print(f"Writing wordcount results to {output_dir}...")
counts.toDF(["word", "count"]).write.mode("overwrite").parquet(output_dir)

print(f"Reading back written Parquet results from {output_dir}...")
df_back = spark.read.parquet(output_dir)
df_back.show(truncate=False)

spark.stop()
print("S3A + Silo + Parquet Test Completed Successfully!")
