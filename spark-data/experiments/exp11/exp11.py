from pyspark.sql import SparkSession
from pyspark import StorageLevel

# 为了快速看到 OOM，我们故意把 Executor 内存设得很小
spark = SparkSession.builder \
    .appName("Exp11_OOM_Lab") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.memory.fraction", "0.2") \
    .config("spark.memory.storageFraction", "0.1") \
    .getOrCreate()

# 1. 构造两个较大的 DF
df_a = spark.range(1, 10000000).selectExpr("id", "id % 1000 as key", "rand() as val")
df_b = spark.range(1, 10000000).selectExpr("id % 1000 as key", "rand() as val_2")

# 2. 强行占用 Storage 内存
print("正在 Cache 大表 A...")
df_a.persist(StorageLevel.MEMORY_ONLY) # 强制不让溢写到磁盘
df_a.count()

# 3. 触发激烈的 Execution 内存争抢
print("正在执行大表 Join（产生 Shuffle）...")
# 这里的 Join 会产生大量的内存占用进行排序和缓冲
result = df_a.join(df_b, "key").count()

print(f"最终结果: {result}")
spark.stop()