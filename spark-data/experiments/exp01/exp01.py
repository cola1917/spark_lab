from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("exp01_job_stage_task")
    .master("spark://spark-master:7077") 
    .getOrCreate()
)

sc = spark.sparkContext

data = sc.parallelize(range(1, 10000000), numSlices=4)

# --- 🧪 Job 1: 窄依赖实验 ---
# 使用 setJobDescription 让你在 Spark UI 的 Job 页面直接看到说明
sc.setJobDescription("Step1: Narrow Dependency (Filter + Count)")

# 简单计算：只有 Stage 0
count_result = data.filter(lambda x: x % 2 == 0).count()


# --- 🧪 Job 2: 宽依赖实验 ---
sc.setJobDescription("Step2: Wide Dependency (ReduceByKey + Collect)")

# 逻辑：强制触发 Shuffle
group_result = (
    data.map(lambda x: (x % 100, 1))
    .reduceByKey(lambda a, b: a + b)  # 🚨 产生 Shuffle，切分 Stage
    .filter(lambda x: x[1] > 0)
    .collect()
)
spark.stop()