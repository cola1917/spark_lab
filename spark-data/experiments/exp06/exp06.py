from pyspark.sql import SparkSession

# 初始化 Spark
spark = SparkSession.builder \
    .appName("Exp07_Join_Final_Comparison") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

# 1. 准备数据
# 大表：100万条订单
large_df = spark.range(1, 1000001).selectExpr(
    "id as order_id", 
    "(id % 100) as city_id", 
    "round(rand() * 100, 2) as price"
)
# 小表：100条城市信息
small_df = spark.range(1, 101).selectExpr(
    "id as city_id", 
    "concat('City_', id) as city_name"
)

# ---------------------------------------------------------
# 场景 A：默认状态 - 触发 BroadcastHashJoin (BHJ)
# ---------------------------------------------------------
# 设置描述，方便在 UI 中快速定位
sc.setJobDescription("Exp07_BHJ: Optimized_No_Shuffle_Join")

# 确保开启广播（默认 10MB）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024) 

print("\n" + "="*50)
print("SCENARIO A: Broadcast Hash Join (Execution)")
print("="*50)

join_bhj = large_df.join(small_df, "city_id")
join_bhj.count()    # 触发 Action


# ---------------------------------------------------------
# 场景 B：禁用广播 - 强制触发 SortMergeJoin (SMJ)
# ---------------------------------------------------------
# 设置描述，模拟两张大表强行 Shuffle 的过程
sc.setJobDescription("Exp07_SMJ: Heavy_Shuffle_And_Sort_Join")

# 禁用广播阈值
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

print("\n" + "="*50)
print("SCENARIO B: Sort Merge Join (Execution)")
print("="*50)

join_smj = large_df.join(small_df, "city_id")
join_smj.count()    # 触发 Action


# ---------------------------------------------------------
# 打印计划对比，用于文字报告总结
# ---------------------------------------------------------
print("\n" + "="*50)
print("PLAN ANALYSIS")
print("="*50)
print("\n[BHJ Plan]:")
join_bhj.explain()

print("\n[SMJ Plan]:")
join_smj.explain()