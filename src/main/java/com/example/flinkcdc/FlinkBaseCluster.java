package com.example.flinkcdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

public class FlinkBaseCluster {
    public static void main(String[] args) throws Exception {
        // 1. 设置 Flink 配置
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081);

        // 2. 核心：手动构建并启动 MiniCluster
        // 这样即使没有 Job，Web UI 也会一直运行
        MiniClusterConfiguration clusterConfig = new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumSlotsPerTaskManager(8) // 设置槽位数量
                .setNumTaskManagers(1)
                .build();

        MiniCluster miniCluster = new MiniCluster(clusterConfig);
        miniCluster.start();

        System.out.println("=================================================");
        System.out.println("企业级模拟集群已启动!");
        System.out.println("Web UI 地址: http://localhost:8081");
        System.out.println("现在你可以向此集群投递任务了");
        System.out.println("=================================================");

        // 3. 阻塞主线程，保持集群存活
        Thread.currentThread().join();
    }
}