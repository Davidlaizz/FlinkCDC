整理了三种不同的 Flink CDC 实现方案，包括环境准备、代码解析、对比分析以及运行注意事项。

---

# Flink CDC MySQL 数据同步方案汇总

本项目展示了三种使用 Flink CDC 将 MySQL 数据实时同步到另一个 MySQL 表（或多个表）的实现方案。项目涵盖了从底层自定义实现到高层 SQL 实现的演进过程。

## 📋 目录

1. [环境准备](https://www.google.com/search?q=%231-%E7%8E%AF%E5%A2%83%E5%87%86%E5%A4%87)
2. [方案一：自定义 Sink (CustomSinkDemo)](https://www.google.com/search?q=%232-%E6%96%B9%E6%A1%88%E4%B8%80%E8%87%AA%E5%AE%9A%E4%B9%89-sink-customsinkdemo)
3. [方案二：Flink SQL 多路同步 (FlinkSQLDemo)](https://www.google.com/search?q=%233-%E6%96%B9%E6%A1%88%E4%BA%8Cflink-sql-%E5%A4%9A%E8%B7%AF%E5%90%8C%E6%AD%A5-flinksqldemo)
4. [方案三：DataStream 分流写法 (JdbcSinkDemo)](https://www.google.com/search?q=%234-%E6%96%B9%E6%A1%88%E4%B8%89datastream-%E5%88%86%E6%B5%81%E5%86%99%E6%B3%95-jdbcsinkdemo)
5. [方案对比与总结](https://www.google.com/search?q=%235-%E6%96%B9%E6%A1%88%E5%AF%B9%E6%AF%94%E4%B8%8E%E6%80%BB%E7%BB%93)
6. [常见问题与注意事项](https://www.google.com/search?q=%236-%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98%E4%B8%8E%E6%B3%A8%E6%84%8F%E4%BA%8B%E9%A1%B9)

---

## 1. 环境准备

在运行代码之前，请确保 MySQL 中存在以下数据库和表结构。

### 数据库配置

* **Host**: `192.168.216.101`
* **Port**: `3306`
* **User**: `root`
* **Password**: `123`
* **Database**: `test`

### SQL 初始化脚本 (请在 MySQL 中执行)

```sql
CREATE DATABASE IF NOT EXISTS test;
USE test;

-- 1. 源表 (Source)
CREATE TABLE IF NOT EXISTS human (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);

-- 2. 目标表 1 (Sink - 全量字段)
CREATE TABLE IF NOT EXISTS human_sink (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);

-- 3. 目标表 2 (Sink - 仅姓名，用于 FlinkSQL 演示)
CREATE TABLE IF NOT EXISTS human_name (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- 插入测试数据
INSERT INTO human VALUES (1, 'Alice', 18), (2, 'Bob', 20);

```

---

## 2. 方案一：自定义 Sink (CustomSinkDemo)

### 简介

这是最底层的实现方式，通过继承 `RichSinkFunction` 手动管理 JDBC 连接和 SQL 执行逻辑。

### 核心逻辑

1. **手动解析 JSON**: 使用 FastJSON 解析 Debezium 生成的 JSON 格式数据。
2. **判断操作类型 (`op`)**:
* `r` (Read/Snapshot): 执行 `TRUNCATE` 清空目标表，然后插入数据。
* `c` (Create): 执行 `INSERT`。
* `u` (Update): 执行 `UPDATE`。
* `d` (Delete): 执行 `DELETE`。


3. **事务控制**: 手动控制 `connection.setAutoCommit(false)` 和 `connection.commit()`。

### 优缺点

* ✅ **优点**: 极高的灵活性，可以在代码中处理复杂的业务逻辑（如清洗、转换、清表操作）。
* ❌ **缺点**: 代码冗余，需要手动处理连接池、异常重试、批处理等问题，容易产生 Bug（如连接泄露）。

---

## 3. 方案二：Flink SQL 多路同步 (FlinkSQLDemo)

### 简介

这是最推荐的生产环境写法，使用 Flink Table API & SQL，声明式地定义源和目标。代码演示了**一次读取，同时写入两个不同下游表**的能力。

### 核心逻辑

1. **DDL 定义**: 使用 `CREATE TABLE` 定义源表 (`mysql-cdc`) 和两个目标表 (`jdbc`)。
2. **列裁剪**:
* `mysql_sink`: 同步完整字段 (`id`, `name`, `age`)。
* `mysql_sink_name`: 仅同步 (`id`, `name`)，自动裁剪掉 `age` 字段。


3. **StatementSet**: 使用 `tEnv.createStatementSet()` 将两条 `INSERT` 语句打包提交。这确保了 Flink 只启动**一个 Job**，源端 Binlog 只被读取一次，分发给两个下游，性能最高。

### 优缺点

* ✅ **优点**: 代码极其简洁，自动处理 Type Mapping，利用官方 Connector 实现了批写入和断点续传，性能好。
* ❌ **缺点**: 相比 DataStream API，自定义复杂逻辑的灵活度稍低（需要写 UDF）。

---

## 4. 方案三：DataStream 分流写法 (JdbcSinkDemo)

### 简介

结合了 DataStream API 的灵活性和官方 `JdbcSink` 的稳定性。解决了官方 `JdbcSink` 只能绑定一条 SQL 的限制。

### 核心逻辑

1. **分流处理 (Split Stream)**:
* **流 A (Upsert)**: 过滤出 `op != 'd'` 的数据，使用 `REPLACE INTO` 语法实现幂等写入（新增或修改）。
* **流 B (Delete)**: 过滤出 `op == 'd'` 的数据，使用 `DELETE FROM` 语法实现物理删除。


2. **配置复用**: 提取了 `JdbcExecutionOptions` (批处理配置) 和 `JdbcConnectionOptions`，使代码更整洁。
3. **数据提取**:
* Upsert 流取 `after` 数据。
* Delete 流取 `before` 数据（因为删除后 `after` 为空）。



### 优缺点

* ✅ **优点**: 既保留了官方 Connector 的可靠性（自动管理连接、批量提交），又实现了完整的增删改同步。
* ❌ **缺点**: 需要对流进行多次 Filter 操作，代码量适中。

---

## 5. 方案对比与总结

| 特性 | CustomSink (自定义) | Flink SQL (SQL) | JdbcSink (分流) |
| --- | --- | --- | --- |
| **开发难度** | 高 (需手写 JDBC) | 低 (写 SQL) | 中 (需配置分流) |
| **代码量** | 多 | 少 | 中 |
| **稳定性** | 依赖开发者水平 | 高 (官方优化) | 高 (官方优化) |
| **删除支持** | 手动编写 DELETE 逻辑 | 自动识别 | 需分流写 DELETE 逻辑 |
| **性能** | 单条执行 (较慢) | 批量提交 (快) | 批量提交 (快) |
| **多路分发** | 需手写逻辑 | `StatementSet` 完美支持 | 需手动添加多个 Sink |
| **推荐场景** | 极度复杂的自定义逻辑 | **常规数据同步/ETL** | **需要代码控制细节的同步** |

---

## 6. 常见问题与注意事项

### 1. JDK 17+ 启动参数报错

如果你使用的是 JDK 17 或更高版本，Flink 需要反射访问 JDK 内部类。请在 IDE 的 VM Options 或启动脚本中添加以下参数：

```bash
--add-opens java.base/java.lang=ALL-UNNAMED

```

### 2. Checkpoint (检查点)

所有代码中都开启了 `env.enableCheckpointing(3000)`。

* **重要**: Flink CDC 依赖 Checkpoint 来记录 Binlog 的 Offset。如果没有 Checkpoint，任务失败重启后可能会丢失进度或无法恢复。
* **注意**: 在 IDEA 本地运行时，停止任务通常意味着状态丢失。生产环境需配置 HDFS/S3 路径保存状态。

### 3. Server ID

MySQL CDC Source 需要配置 `server-id`。

* 在代码中配置为 `'server-id' = '5800-5900'`。
* **注意**: 在同一个 MySQL 实例上运行多个 Flink CDC 任务时，每个任务的 `server-id` 必须唯一，不能冲突。

### 4. JDBC Sink 的幂等性

* **Flink SQL**: 内部自动处理，通常依赖主键。
* **JdbcSinkDemo**: 使用了 `REPLACE INTO`。请确保 MySQL 表有主键或唯一索引，否则 `REPLACE INTO` 会变成单纯的 `INSERT`，导致数据重复。