from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, udf
from pyspark.sql.types import StringType
import time

# 1. 初始化Spark，显式配置Arrow优化，禁用不必要的缓存
spark = SparkSession.builder \
    .appName("Exp14_UDF_sql_pandas_Lab") \
    .master("local[*]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.cache.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .getOrCreate()
sc = spark.sparkContext

# 2. 定义通用函数：生成全新的DataFrame（避免复用缓存）+ 执行方案 + 返回耗时
def test_performance(plan_name, process_func):
    """
    通用性能测试函数
    :param plan_name: 方案名称
    :param process_func: 数据处理函数（输入df，返回处理后的df）
    :return: 处理耗时
    """
    # 关键：每次测试都生成**全新的DataFrame**，避免复用前面的缓存
    df = spark.range(0, 10000000, numPartitions=1000).select(col("id").cast("string").alias("val"))
    
    # 关键：预热（先执行一次小型数据处理，规避JVM预热成本）
    df_small = spark.range(0, 1000).select(col("id").cast("string").alias("val"))
    process_func(df_small).count()
    
    # 设置Job描述
    sc.setJobDescription(plan_name)
    
    # 执行真实测试
    start = time.time()
    # 关键：使用`write.format("noop").save()`替代`count()`，避免Spark优化掉列处理逻辑
    # noop格式表示「不写入任何数据」，但会执行完整的列处理逻辑
    process_func(df).write.format("noop").mode("overwrite").save()
    end = time.time()
    
    # 关键：强制清除当前df的缓存，避免影响后续测试
    df.unpersist(blocking=True)
    spark.catalog.clearCache()
    
    cost = end - start
    print(f"{plan_name} 耗时: {cost:.2f}s")
    return cost

# 3. 定义三种方案的处理函数
# 方案A：原生内置函数
def process_native(df):
    return df.withColumn("res", concat(col("val"), lit("_native")))

# 方案B：Python UDF（Row-by-row）
def my_concat(s):
    return s + "_udf"
concat_udf = udf(my_concat, StringType())
def process_python_udf(df):
    return df.withColumn("res", concat_udf(col("val")))

# 方案C：Pandas UDF（Vectorized - 使用 Arrow）
from pyspark.sql.functions import pandas_udf
import pandas as pd
@pandas_udf(StringType())
def vectorized_concat(s: pd.Series) -> pd.Series:
    return s + "_pandas"
def process_pandas_udf(df):
    return df.withColumn("res", vectorized_concat(col("val")))

# 4. 执行性能测试（按顺序执行，每次生成全新DataFrame）
if __name__ == "__main__":
    # 先执行一次全局预热，规避集群资源申请成本
    df_warmup = spark.range(0, 10000).select(col("id").cast("string").alias("val"))
    df_warmup.withColumn("res", concat(col("val"), lit("_warmup"))).count()
    
    # 执行三种方案（每次测试都是独立的，无缓存复用）
    test_performance("EXP14_A: Native Spark Functions", process_native)
    test_performance("EXP14_B: Python UDF (Standard)", process_python_udf)
    test_performance("EXP14_C: Pandas UDF (Vectorized)", process_pandas_udf)
    
    # 停止Spark
    spark.stop()