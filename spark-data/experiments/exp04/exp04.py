from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# SparkSession
spark = (
    SparkSession.builder
    .appName("exp04_rdd_skew")
    .master("spark://spark-master:7077")  # 确保将 master 配置为你的 Spark 集群地址
    .getOrCreate()
)

sc = spark.sparkContext

# --- 1. 制造倾斜数据 ---
# 99000 条数据都是 "Beijing"，只有 100 条是其他城市
skewed_data = [("Beijing", 1)] * 1000000 + [(f"City_{i}", 1) for i in range(100)]

# --- 2. 触发 Shuffle 操作 ---
sc.setJobDescription("Exp04: RDD_Skew_Test")
rdd = sc.parallelize(skewed_data, 10)
# 模拟一个慢速计算函数
# 不然数据不够震撼
def slow_func(iterator):
    for x in iterator:
        # 如果是热点数据，故意让它算得慢
        if x[0] == "Beijing":
            # 模拟复杂计算：比如执行 100 万次无意义循环
            sum(range(1000)) 
        yield x


rdd_skewed = rdd.mapPartitions(slow_func)

# reduceByKey 会触发 Shuffle，相同的 Key 会进入同一个 Task
result = rdd_skewed.reduceByKey(lambda a, b: a + b).collect()
spark.stop()