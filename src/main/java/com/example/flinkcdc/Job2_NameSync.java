package com.example.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 任务 2：列裁剪同步
 * 仅同步 id 和 name 两个字段到 mysql_sink_name 表
 */
public class Job2_NameSync {
    public static void main(String[] args) throws Exception {
        // 1. 指向已经手动启动的 MiniCluster (8081)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 8081);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建 Source 表：读取 MySQL CDC
        // 注意：server-id 设为 5802，与 Job1 (5801) 区分开，防止互相踢掉连接
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
                        "  'server-id' = '5802'" +
                        ")"
        );

        // 3. 创建 Sink 表：仅包含 id 和 name
        tEnv.executeSql(
                "CREATE TABLE mysql_sink_name (" +
                        "  id INT PRIMARY KEY NOT ENFORCED," +
                        "  name STRING" +
                        ") WITH (" +
                        "  'connector' = 'jdbc'," +
                        "  'url' = 'jdbc:mysql://192.168.216.101:3306/test'," +
                        "  'table-name' = 'human_name'," + // 目标表名不同
                        "  'username' = 'root'," +
                        "  'password' = '123'," +
                        "  'sink.buffer-flush.max-rows' = '100'," +
                        "  'sink.buffer-flush.interval' = '1s'" +
                        ")"
        );

        // 4. 执行同步：进行列裁剪（不选 age 字段）
        System.out.println("Job 2 正在投递到 8081 集群...");

        // 此处提交后，Flink 客户端会将 JobGraph 发送给 BaseCluster
        tEnv.executeSql("INSERT INTO mysql_sink_name SELECT id, name FROM mysql_source");
    }
}