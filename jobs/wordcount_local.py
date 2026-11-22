"""Phase 1 Local Mode baseline; run from the Notebook terminal or a cell."""

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("WordCount-Local").master("local[*]").getOrCreate()
lines = spark.sparkContext.parallelize(
    ["spark kubernetes", "spark notebook", "pyspark spark"], 2
)
counts = lines.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(
    lambda left, right: left + right
)
print(sorted(counts.collect()))
spark.stop()
