from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# SparkSession
# EXPLAIN + 执行计划分析实验（核心）
spark = (
    SparkSession.builder
    .appName("exp05_explain_analysis")
    .master("spark://spark-master:7077")  # 确保将 master 配置为你的 Spark 集群地址
    .getOrCreate()
)

sc = spark.sparkContext
# 创建大表
orders = [(i, i % 10, i * 1.5) for i in range(1000000)]
orders_df = spark.createDataFrame(orders, ["order_id", "city_id", "price"])

# 创建小表
cities = [(i, f"City_{i}") for i in range(100)]
city_df = spark.createDataFrame(cities, ["city_id", "city_name"])

# 注册为 SQL 临时表
orders_df.createOrReplaceTempView("orders")
city_df.createOrReplaceTempView("cities")
# 故意写一个看起来比较复杂的 SQL
sql_query = """
SELECT c.city_name, SUM(o.price) as total_sales
FROM orders o
JOIN cities c ON o.city_id = c.city_id
WHERE o.price > 100
GROUP BY c.city_name
"""

# 核心步骤：打印全生命周期计划
# True 会展示 Parsed, Analyzed, Optimized 和 Physical 四阶段
spark.sql(sql_query).count()  # 先执行一次，触发计划生成
spark.stop()