package com.example.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLDemo {
    public static void main(String[] args) {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 必须开启 Checkpoint
        env.enableCheckpointing(3000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建 Source 表：读取 MySQL CDC
        tEnv.executeSql(
                "CREATE TABLE mysql_source (" +
                        "  id INT PRIMARY KEY NOT ENFORCED," +
                        "  name STRING," +
                        "  age INT" +
                        ") WITH (" +
                        "  'connector' = 'mysql-cdc'," +
                        "  'hostname' = '192.168.216.101'," +
                        "  'port' = '3306'," +
                        "  'username' = 'root'," +
                        "  'password' = '123'," +
                        "  'database-name' = 'test'," +
                        "  'table-name' = 'human'," +
                        "  'server-id' = '5800-5900'" + // 读取端需要 server-id
                        ")"
        );

        // 3. 创建 Sink 表 1：原来的 human_sink (包含 age)
        tEnv.executeSql(
                "CREATE TABLE mysql_sink (" +
                        "  id INT PRIMARY KEY NOT ENFORCED," +
                        "  name STRING," +
                        "  age INT" +
                        ") WITH (" +
                        "  'connector' = 'jdbc'," +
                        "  'url' = 'jdbc:mysql://192.168.216.101:3306/test'," +
                        "  'table-name' = 'human_sink'," +
                        "  'username' = 'root'," +
                        "  'password' = '123'," +
                        "  'sink.buffer-flush.max-rows' = '500'," +
                        "  'sink.buffer-flush.interval' = '1s'" +
                        ")"
        );

        // 4. 创建 Sink 表 2：新的 human_name (只有 id, name，去除 age)
        // 注意：运行前请确保 MySQL 中已存在表 human_name
        tEnv.executeSql(
                "CREATE TABLE mysql_sink_name (" +
                        "  id INT PRIMARY KEY NOT ENFORCED," +
                        "  name STRING" +
                        ") WITH (" +
                        "  'connector' = 'jdbc'," +
                        "  'url' = 'jdbc:mysql://192.168.216.101:3306/test'," +
                        "  'table-name' = 'human_name'," +
                        "  'username' = 'root'," +
                        "  'password' = '123'," +
                        "  'sink.buffer-flush.max-rows' = '500'," +
                        "  'sink.buffer-flush.interval' = '1s'" +
                        ")"
        );

        // 5. 执行同步：使用 StatementSet 同时提交两个 INSERT 任务
        // 这种方式比写两个 tEnv.executeSql() 更好，因为它只会启动一个 Flink Job，
        // 源数据只读取一次，然后分流给两个 Sink，性能更高。
        StatementSet statementSet = tEnv.createStatementSet();

        // 任务 A：完整同步到 human_sink
        statementSet.addInsertSql("INSERT INTO mysql_sink SELECT id, name, age FROM mysql_source");

        // 任务 B：列裁剪同步到 human_name
        statementSet.addInsertSql("INSERT INTO mysql_sink_name SELECT id, name FROM mysql_source");

        // 提交执行
        statementSet.execute();
    }
}