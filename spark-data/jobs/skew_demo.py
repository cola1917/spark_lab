# test_parallelism.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("parallelism-test").getOrCreate()

rdd = spark.sparkContext.parallelize(range(1000), 10)

def slow(x):
    import time
    time.sleep(0.1)
    return x

rdd.map(slow).count()

spark.stop()
