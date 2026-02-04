from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Exp07_SQL_Skew_AQE") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

sc = spark.sparkContext

# 1. 构造倾斜数据
# 大表：100万行，其中 80% 是 city_id = 1 (北京)
skewed_data = [(1, i) for i in range(800000)] + [(i % 100, i) for i in range(200000)]
df_large = spark.createDataFrame(skewed_data, ["city_id", "order_id"])

# 字典表：正常分布
df_city = spark.createDataFrame([(i, f"City_{i}") for i in range(100)], ["city_id", "city_name"])

# 2. 实验 A：观察 AQE 自动处理倾斜
sc.setJobDescription("Exp07_SQL: AQE_Auto_Skew_Join")
result_aqe = df_large.join(df_city, "city_id")
result_aqe.count()

# 禁用自动广播以确保使用 Shuffle Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# 3. 实验 B：手动加盐 (Salting) 对比
sc.setJobDescription("Exp07_SQL: Manual_Salting_Join")
# 为大表 key 加盐 (0-9 随机数)
df_large_salted = df_large.withColumn("salted_key", F.concat(F.col("city_id"), F.lit("_"), F.expr("floor(rand() * 10)")))
# 为小表扩容 (每个 key 复制 10 份)
df_city_salted = df_city.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(10)]))) \
                        .withColumn("salted_key", F.concat(F.col("city_id"), F.lit("_"), F.col("salt")))

result_salting = df_large_salted.join(df_city_salted, "salted_key")
result_salting.count()
spark.stop()