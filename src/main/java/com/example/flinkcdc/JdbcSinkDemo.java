package com.example.flinkcdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        // 1. Source 配置保持不变
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.216.101")
                .port(3306)
                .databaseList("test")
                .tableList("test.human")
                .username("root")
                .password("123")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .serverId("1")
                .build();

        DataStreamSource<String> rawStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        // 设置并行度为1，方便观察日志顺序
        rawStream.setParallelism(1);

        // -------------------------------------------------------------------------
        // 提取公共配置：为了代码整洁，把 Sink 的执行参数和连接参数提出来复用
        // -------------------------------------------------------------------------

        // A. 执行参数（批量提交配置）
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(500)             // 攒够500条提交
                .withBatchIntervalMs(1000)      // 或每1秒提交
                .withMaxRetries(3)
                .build();

        // B. 连接参数
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://192.168.216.101:3306/test")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root")
                .withPassword("123")
                .build();

        // -------------------------------------------------------------------------
        // 核心改造：分流处理
        // -------------------------------------------------------------------------

        // 分支 1：处理 新增(c)、读取(r)、更新(u) -> 执行 REPLACE INTO
        rawStream
                .filter(value -> {
                    String op = JSON.parseObject(value).getString("op");
                    return !"d".equals(op); // 只要 op 不是 d，都走这条路
                })
                .addSink(JdbcSink.sink(
                        "REPLACE INTO test.human_sink (id, name, age) VALUES (?, ?, ?)",
                        (ps, value) -> {
                            JSONObject json = JSON.parseObject(value);
                            // 增改查的数据都在 'after' 字段里
                            JSONObject data = json.getJSONObject("after");
                            if (data != null) {
                                ps.setInt(1, data.getInteger("id"));
                                ps.setString(2, data.getString("name"));
                                ps.setInt(3, data.getInteger("age"));
                            }
                        },
                        executionOptions, // 复用配置
                        connectionOptions // 复用配置
                ));

        // 分支 2：处理 删除(d) -> 执行 DELETE FROM
        rawStream
                .filter(value -> {
                    String op = JSON.parseObject(value).getString("op");
                    return "d".equals(op); // 只有 op 是 d，才走这条路
                })
                .addSink(JdbcSink.sink(
                        "DELETE FROM test.human_sink WHERE id = ?",
                        (ps, value) -> {
                            JSONObject json = JSON.parseObject(value);
                            // 删除的数据在 'before' 字段里
                            JSONObject data = json.getJSONObject("before");
                            if (data != null) {
                                ps.setInt(1, data.getInteger("id"));
                            }
                        },
                        executionOptions, // 复用配置
                        connectionOptions // 复用配置
                ));

        env.execute("JdbcSink_Split_Stream_Demo");
    }
}