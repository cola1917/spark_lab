from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Exp09_Cache_Persist") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

sc = spark.sparkContext

# 1. 构造 DF
df_raw = spark.range(1, 10000000).selectExpr(
    "id", 
    "sin(id) as val_a", 
    "cos(id) * id as val_b"
).repartition(10)

# --- 场景 A：无 Cache（观察重复计算） ---
sc.setJobDescription("Step1_NoCache_FirstAction")
df_raw.count()

sc.setJobDescription("Step2_NoCache_SecondAction_Recompute")
df_raw.filter("val_a > 0").count()

# --- 场景 B：有 Cache（观察内存加速） ---
df_raw.cache() # 懒执行，不产生 Job

sc.setJobDescription("Step3_WithCache_FirstAction_FillMemory")
df_raw.count() 

sc.setJobDescription("Step4_WithCache_SecondAction_HitMemory")
df_raw.filter("val_a > 0").count()

# 清理
df_raw.unpersist()
spark.stop()