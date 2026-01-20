package com.example.flinkcdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

// Flink 的一些核心机制（如序列化、ClosureCleaner 闭包清理）需要通过“暴力反射”来访问 JDK 的内部字段（例如 java.lang.String.value）。
// JDK 17 默认禁止了这种行为，除非你显式地在启动参数中“打开”这些包。 --add-opens java.base/java.lang=ALL-UNNAMED

public class CustomSinkDemo {
    public static void main(String[] args) throws Exception {
        // flink source，source类型为mysql
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

        // 初始化环境.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        DataStreamSource<String> stringDataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 1 parallel source tasks
                .setParallelism(1);

        // 将数据打印到客户端.
        stringDataStreamSource
                .print().setParallelism(1); // use parallelism 1 for sink

        // 数据同步到mysql
        stringDataStreamSource.addSink(new RichSinkFunction<String>() {
            private Connection connection = null;
            private PreparedStatement preparedStatement = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                if (connection == null) {
                    Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
                    connection = DriverManager.getConnection("jdbc:mysql://192.168.216.101:3306", "root", "123");//获取连接
                    connection.setAutoCommit(false);//关闭自动提交
                }
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String op = jsonObject.getString("op");
                if ("r".equals(op)) { // 首次全量
                    System.out.println("执行清表操作");
                    connection.prepareStatement("truncate table test.human_sink").execute(); // 清空目标表数据
                    JSONObject after = jsonObject.getJSONObject("after");
                    Integer id = after.getInteger("id");
                    String name = after.getString("name");
                    Integer age = after.getInteger("age");
                    preparedStatement = connection.prepareStatement("insert into test.human_sink values (?,?,?)");
                    preparedStatement.setInt(1, id);
                    preparedStatement.setString(2, name);
                    preparedStatement.setInt(3, age);
                    preparedStatement.execute();
                    connection.commit();//预处理完成后统一提交
                }else if("c".equals(op)) { // 新增.
                    JSONObject after = jsonObject.getJSONObject("after");
                    Integer id = after.getInteger("id");
                    String name = after.getString("name");
                    Integer age = after.getInteger("age");
                    preparedStatement = connection.prepareStatement("insert into test.human_sink values (?,?,?)");
                    preparedStatement.setInt(1, id);
                    preparedStatement.setString(2, name);
                    preparedStatement.setInt(3, age);
                    preparedStatement.execute();
                    connection.commit();//预处理完成后统一提交
                }
                else if ("d".equals(op)) { // 删除
                    JSONObject after = jsonObject.getJSONObject("before");
                    Integer id = after.getInteger("id");
                    preparedStatement = connection.prepareStatement("delete from test.human_sink where id = ?");
                    preparedStatement.setInt(1, id);
                    preparedStatement.execute();
                    connection.commit();//预处理完成后统一提交
                } else if ("u".equals(op)) { // 更新
                    JSONObject after = jsonObject.getJSONObject("after");
                    Integer id = after.getInteger("id");
                    String name = after.getString("name");
                    Integer age = after.getInteger("age");
                    preparedStatement = connection.prepareStatement("update test.human_sink set name = ?, age = ? where id = ?");
                    preparedStatement.setString(1, name);
                    preparedStatement.setInt(2, age);
                    preparedStatement.setInt(3, id);
                    preparedStatement.execute();
                    connection.commit();//预处理完成后统一提交
                } else {
                    System.out.println("不支持的操作op=" + op);
                }
            }

            @Override
            public void close() throws Exception {
                System.out.println("执行close方法");
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        });

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
