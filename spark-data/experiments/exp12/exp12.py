from pyspark.sql import SparkSession
import time

# 初始化 Spark
spark = SparkSession.builder \
    .appName("Exp12_SmallFiles_Optimization") \
    .master("spark://spark-master:7077") \
    .getOrCreate()
sc = spark.sparkContext

# --- 阶段 1: 制造小文件 ---
# 在 UI 中你会看到：EXP12_STAGE_1: Generating 1000 Small Files
sc.setJobDescription("EXP12_STAGE_1: Generating 1000 Small Files")
df = spark.range(0, 100000).toDF("id")
# 故意强行重分区为 1000，产生 1000 个碎文件
df.repartition(1000).write.mode("overwrite").parquet("/tmp/exp12/small_files")

# --- 阶段 2: 测试小文件读取性能 ---
# 在 UI 中你会看到：EXP12_STAGE_2: Reading 1000 Small Files (Benchmark)
sc.setJobDescription("EXP12_STAGE_2: Reading 1000 Small Files (Benchmark)")
start_small = time.time()
count_small = spark.read.parquet("/tmp/exp12/small_files").count()
end_small = time.time()
print(f"读取 1000 个小文件耗时: {end_small - start_small:.2f} 秒")

# --- 阶段 3: 治理小文件 (合并) ---
# 在 UI 中你会看到：EXP12_STAGE_3: Merging Small Files using Coalesce(1)
sc.setJobDescription("EXP12_STAGE_3: Merging Small Files using Coalesce(1)")
df_small = spark.read.parquet("/tmp/exp12/small_files")
# 关键：使用 coalesce(1) 减少分区而不触发 Shuffle
df_small.coalesce(1).write.mode("overwrite").parquet("/tmp/exp12/merged_file")

# --- 阶段 4: 测试合并后读取性能 ---
# 在 UI 中你会看到：EXP12_STAGE_4: Reading Merged File (Benchmark)
sc.setJobDescription("EXP12_STAGE_4: Reading Merged File (Benchmark)")
start_merged = time.time()
count_merged = spark.read.parquet("/tmp/exp12/merged_file").count()
end_merged = time.time()
print(f"读取合并后大文件耗时: {end_merged - start_merged:.2f} 秒")