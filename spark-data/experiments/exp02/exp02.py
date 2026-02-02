from pyspark.sql import SparkSession

# SparkSession
spark = (
    SparkSession.builder
    .appName("exp02_shuffle_experiment")
    .master("spark://spark-master:7077")  # 确保将 master 配置为你的 Spark 集群地址
    .getOrCreate()
)

sc = spark.sparkContext

# 2️⃣ 创建基础数据
# 用一个包含多个键值对的数据创建 RDD
rdd = sc.parallelize([(1, 'a'), (2, 'b'), (1, 'c'), (2, 'd'), (3, 'e')], 4)

# --- 窄依赖实验：不触发 Shuffle ---
# `filter` 是窄依赖操作，不会引发 Shuffle
sc.setJobDescription("Narrow Dependency Experiment: Filter")
narrow_result = rdd.filter(lambda x: x[0] == 1).collect()

# --- 宽依赖实验：触发 Shuffle ---
# `groupByKey` 是宽依赖操作，会触发 Shuffle
sc.setJobDescription("Wide Dependency Experiment: groupByKey")
group_result = rdd.groupByKey().collect()

# --- 宽依赖实验：触发 Shuffle ---
# `reduceByKey` 是宽依赖操作，会触发 Shuffle
sc.setJobDescription("Wide Dependency Experiment: ReduceByKey")
reduce_result = rdd.reduceByKey(lambda x, y: x + y).collect()

# --- 宽依赖实验：触发 Shuffle ---
# `join` 是宽依赖操作，会触发 Shuffle
rdd2 = sc.parallelize([(1, 'x'), (2, 'y'), (3, 'z')], 4)
sc.setJobDescription("Wide Dependency Experiment: Join")
join_result = rdd.join(rdd2).collect()

# --- 宽依赖实验：触发 Shuffle ---
# `distinct` 是宽依赖操作，会触发 Shuffle
sc.setJobDescription("Wide Dependency Experiment: Distinct")
distinct_result = rdd.map(lambda x: x[0]).distinct().collect()

# --- 宽依赖实验：触发 Shuffle ---
# `cartesian` 是宽依赖操作，会触发 Shuffle
rdd1 = sc.parallelize([1, 2])
rdd2 = sc.parallelize([3, 4])
sc.setJobDescription("Wide Dependency Experiment: Cartesian")
cartesian_result = rdd1.cartesian(rdd2).collect()

# --- 宽依赖实验：触发 Shuffle ---
# `repartition` 会重新分区，通常触发 Shuffle
sc.setJobDescription("Wide Dependency Experiment: Repartition")
repartition_result = rdd.repartition(5).collect()

# 结束 SparkContext
spark.stop()
