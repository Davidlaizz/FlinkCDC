package com.example.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job1_FullSync {
    public static void main(String[] args) throws Exception {
        // 1. 指向已经启动的 BaseCluster (8081)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 8081);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 2. 创建 Source 表：必须写完整字段，不能写 ...
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
                        "  'server-id' = '5801'" + // Job1 使用 5801
                        ")"
        );

        // 3. 创建 Sink 表
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
                        "  'password' = '123'" +
                        ")"
        );

        // 4. 执行同步
        System.out.println("Job 1 正在投递到集群...");
        tEnv.executeSql("INSERT INTO mysql_sink SELECT id, name, age FROM mysql_source");
    }
}