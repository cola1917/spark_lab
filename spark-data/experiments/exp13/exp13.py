from pyspark.sql import SparkSession
import time

# 1. 初始化 Spark 本地模式（明确指定local[*]，避免歧义）
spark = SparkSession.builder \
    .appName("Exp13_Checkpoint_vs_Cache") \
    .master("local[*]") \
    .getOrCreate()
sc = spark.sparkContext

# 2. 关键修改：使用非/tmp的自定义目录作为Checkpoint目录（避免被自动清理）
# 选择当前工作目录下的checkpoint目录，确保有写入权限，且不会被自动清理
checkpoint_dir = "./spark_checkpoint_local"
sc.setCheckpointDir(checkpoint_dir)
print(f"Checkpoint目录已设置为：{checkpoint_dir}")

# 3. 构造RDD + 关键修改：标记checkpoint后，立即执行一次cache（固化标记，PySpark专属解决方案）
# 先构造RDD
data = sc.parallelize(range(100), 5)
rdd_base = data.map(lambda x: x + 1).map(lambda x: x * 2)

# 核心步骤：先标记checkpoint，再cache（强制固化RDD标记，避免被惰性求值覆盖）
rdd_base.checkpoint()
rdd_base.cache()  # 这一步是PySpark中让checkpoint生效的关键（本地/集群都适用）

# --- 场景 A: 原始状态 ---
print("\n" + "="*20 + " 场景 A: 原始 Lineage " + "="*20)
print(rdd_base.toDebugString().decode())

# --- 场景 B: 使用 Cache ---
sc.setJobDescription("EXP13_CACHE_TEST")
# 直接count，触发缓存
rdd_base.count()
print("\n" + "="*20 + " 场景 B: Cache 后的 Lineage " + "="*20)
print(rdd_base.toDebugString().decode())

# 彻底清除缓存，阻塞式执行
rdd_base.unpersist(blocking=True)
print("\n缓存已彻底清除")

# --- 场景 C: 使用 Checkpoint（关键：确保标记生效+写入完成）---
sc.setJobDescription("EXP13_CHECKPOINT_TEST")

# 1. 第一次count：触发原始RDD计算 + 异步写入Checkpoint（此时有cache辅助，标记不会失效）
print("\n第一次count（触发Checkpoint写入）...")
rdd_base.count()

# 2. 关键：等待5秒（确保异步写入完成，本地模式速度慢，给足时间）
time.sleep(5)

# 3. 第二次count：读取Checkpoint数据，血缘已截断
print("\n第二次count（读取Checkpoint数据）...")
rdd_base.count()

# 4. 打印最终血缘
print("\n" + "="*20 + " 场景 C: Checkpoint 后的 Lineage " + "="*20)
print(rdd_base.toDebugString().decode())

# 5. 辅助验证：打印Checkpoint目录是否有文件（验证写入成功）
import os
if os.path.exists(checkpoint_dir):
    print(f"\nCheckpoint目录写入成功，目录内容：{os.listdir(checkpoint_dir)}")
else:
    print("\nCheckpoint目录写入失败！")

spark.stop()