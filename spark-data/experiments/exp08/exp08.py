from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Exp08_Partition_Tuning") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

# 1. 准备数据：初始 2 个分区
df = spark.range(1, 1000001).repartition(2)
print(f"初始分区数: {df.rdd.getNumPartitions()}")

# --- 场景 A：Repartition (增加分区) ---
# 目的：观察 Shuffle 产生
sc.setJobDescription("Exp08_Part1: Repartition_to_10_Increase")
df_repart = df.repartition(10)
df_repart.count() 

# --- 场景 B：Coalesce (减少分区) ---
# 目的：观察无 Shuffle 减少分区
sc.setJobDescription("Exp08_Part2: Coalesce_to_1_Decrease")
df_coalesce = df_repart.coalesce(1)
df_coalesce.count()

# --- 场景 C：Repartition (减少分区) ---
# 目的：对比 Coalesce，观察减少分区时的 Shuffle
sc.setJobDescription("Exp08_Part3: Repartition_to_1_Decrease")
df_repart_down = df_repart.repartition(1)
df_repart_down.count()

spark.stop()