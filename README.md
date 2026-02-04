# Spark Lab: 从底层实验到工业调优实战

[![Spark Version](https://img.shields.io/badge/Spark-3.2+-red.svg)](https://spark.apache.org/)
[![Python Version](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)

> **Spark Lab** 是一个以“实验驱动调优”为核心的开源项目。它不仅涵盖了 80%–90% 的 Spark 面试核心原理，更是一个真实调优案例的知识库。
>
> **项目初衷**：通过可复现的“母实验”理解 Spark 执行细节，拒绝死记硬背。

---

## 📂 项目结构

项目分为两个核心模块：

### 🔬 1. Experiments (原理验证实验室)
包含 14 个精心设计的“母实验”，每个实验都配有可运行代码，旨在通过 Spark UI 观察底层变化：
- **执行模型**：Job/Stage/Task 拆解、Shuffle 触发机制。
- **数据倾斜**：RDD 层与 SQL 层的各种倾斜场景复现。
- **Join 策略**：BHJ、SMJ 的物理计划对比与 AQE 动态切换。
- **稳定性**：OOM 制造机、内存配比验证、小文件治理。

### 🚀 2. Cases (调优实战记录)
汇集来自真实生产环境（如自动驾驶数据闭环、大规模 ETL）的调优案例。
- **内容涵盖**：现象描述、堆栈日志、调优手段、性能对比。
- **贡献指南**：欢迎提交 PR 贡献案例，请务必**脱敏处理**（隐藏公司名、敏感 ID 及业务私密逻辑）。

---

## 🗺️ 路线图 (Roadmap)

### v1.0 - 原理地基 (Current)
- [x] 完成 14 个核心母实验代码编写。
- [x] 搭建基础项目骨架。

### v2.0 - 实验笔记强化 (Incoming)
- [ ] 为每个 Experiment 补充详细的 `note.md`。
- [ ] 包含：Spark UI 关键截图分析、逻辑执行计划（Logical Plan）与物理计划（Physical Plan）深度解读。
- [ ] 总结对应实验相关的“面试金句”。

### v3.0 - 生态案例库 (Continuous)
- [ ] 持续收集并更新 `cases/` 目录。
- [ ] 引入更多进阶场景：Delta Lake 增量处理、Spark on Kubernetes 性能调优、大规模长周期任务稳定性治理。

---

## 🛠️ 如何开始

没问题，这部分专门针对你的工程架构（Docker + `submit.sh` + `requirements.txt`）进行了深度定制。

它不仅清晰地指引了如何运行，更在字里行间展示了你对**容器化部署**和**生产环境脚本规范**的理解，这对面试“数据平台/数据研发”岗位非常有帮助。

---

## 🛠️ 如何开始 (Getting Started)

本仓库提供完整的 Docker 化环境，确保所有实验结果在不同机器上具有**高度可复现性**，模拟真实生产集群的提交逻辑。

### 1. 环境拉起 (Environment Setup)

项目根目录下内置了 `docker-compose.yml`，一键启动包含 Spark Master, Worker 及 History Server 的全套实验环境。

```bash
# 启动实验容器
# 初次运行会自动拉取基础镜像，确保具备 PySpark 及 Java 运行环境
docker-compose up -d

```

> **注意**：默认使用项目定制镜像。如需自定义，请修改 `docker-compose.yml` 中的 `image` 标签。

### 2. 依赖安装 (Dependencies)

在宿主机进行代码调试或开发时，建议同步实验所需的 Python 依赖库：

```bash
pip install -r requirements.txt

```

### 3. 运行实验 (Running Experiments)

每个实验文件夹（`experiments/lab-xx`）内均包含一个便捷的 `submit.sh` 脚本。该脚本封装了标准生产环境的 `spark-submit` 逻辑，并预设了该实验的核心调优参数。

以 **Lab 06: Join 策略对比实验** 为例：

```bash
# 1. 进入对应实验目录
cd experiments/lab06-join-strategies

# 2. 赋予脚本执行权限
chmod +x submit.sh

# 3. 提交实验任务到容器集群
./submit.sh

```

### 4. 实验观测 (Observation)

实验运行期间或结束后，请通过以下入口观察 Spark 引擎的“执行细节”：

* **Spark UI**: `http://localhost:4040` (任务运行时的实时监控，查看并行度、内存占用)
* **History Server**: `http://localhost:18080` (任务结束后的回溯，分析静态与动态执行计划)

---

### 📝 提交脚本规范说明 (submit.sh)

为了贴近实战，每个 `submit.sh` 均包含以下关键配置，你可以在脚本中手动开关它们来观察性能差异：

```bash
# 示例配置项
--conf "spark.sql.adaptive.enabled=true" \          # 开启/关闭 AQE
--conf "spark.sql.autoBroadcastJoinThreshold=10MB" \ # 调整广播阈值
--conf "spark.memory.fraction=0.6" \               # 模拟内存压力

```