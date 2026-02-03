from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# SparkSession
spark = (
    SparkSession.builder
    .appName("exp03_rdd_vs_dataframe")
    .master("spark://spark-master:7077")  # 确保将 master 配置为你的 Spark 集群地址
    .getOrCreate()
)

sc = spark.sparkContext

# --- 1. 数据准备 (无需读文件) ---
data = [(i, "city_" + str(i % 10), i % 100) for i in range(10000)]

# --- 2. RDD 实验：手动过滤聚合 ---
# Description: RDD 处理 Python 对象，Spark 无法优化内部的 lambda 逻辑
rdd = sc.parallelize(data)
rdd_result = rdd.filter(lambda x: x[2] > 20) \
                .map(lambda x: (x[1], 1)) \
                .reduceByKey(lambda a, b: a + b)
print("RDD Count:", rdd_result.count())

# --- 3. DataFrame 实验：逻辑下推对比 ---
# Description: 使用 DSL。你会看到 Catalyst 如何合并操作并利用 Tungsten 加速
df = spark.createDataFrame(data, ["id", "city", "age"])
df_filtered = df.filter("age > 20").groupBy("city").count()

print("\n" + "="*20 + " DataFrame Physical Plan " + "="*20)
df_filtered.explain() # 提交集群后查看 stdout
df_filtered.collect()
spark.stop()