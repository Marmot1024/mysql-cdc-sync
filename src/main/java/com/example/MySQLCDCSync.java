package com.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLCDCSync {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLCDCSync.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting MySQL CDC Sync application...");
        
        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 配置MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.12.55.40")
                .port(40000)
                .databaseList("cos_power_order_db")
                .tableList("cos_power_order_db.rent_new_agree_tb") 
                .username("dev_dbo_test")
                .password("B4DDC0B232D0E997")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        LOG.info("MySQL CDC Source configured successfully");

        // 3. 添加Source到环境中
        DataStreamSource<String> stream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source");

        // 4. 添加自定义Sink
        stream.addSink(new HiveParseTableSink());

        LOG.info("Starting Flink job...");
        // 5. 执行任务
        env.execute("MySQL CDC to Hive Sync");
    }
} 