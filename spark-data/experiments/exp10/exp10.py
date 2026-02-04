from pyspark.sql import SparkSession
import os
import socket

spark = SparkSession.builder \
    .appName("Exp10_Architecture_Observation") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

sc = spark.sparkContext

# --- 环节 1：Driver 端的打印 ---
print("\n" + "="*50)
print(f"【Driver 视角】")
print(f"我正在机器上执行: {socket.gethostname()}")
print(f"我的进程 ID 是: {os.getpid()}")
print("="*50 + "\n")

# --- 环节 2：Executor 端的打印 ---
def executor_task(x):
    # 获取执行环境信息
    from pyspark import TaskContext
    ctx = TaskContext.get()
    
    # 打印到 Executor 的标准输出（注意：控制台看不见这里！）
    print(f"--- [Executor 任务启动] ---")
    print(f"处理数据: {x}")
    print(f"Hostname: {socket.gethostname()}")
    print(f"PID: {os.getpid()}")
    print(f"Stage ID: {ctx.stageId()}, Partition ID: {ctx.partitionId()}")
    return f"Result from {socket.gethostname()} (PID: {os.getpid()})"

# 构造 2 个分区，确保 2 个 Worker 都能分到任务
rdd = sc.parallelize(range(5), numSlices=2)

# 触发计算
results = rdd.map(executor_task).collect()

# --- 环节 3：结果汇总 ---
print("\n" + "="*50)
print("【Driver 最终收到的结果】")
for res in results:
    print(res)
print("="*50)

spark.stop()