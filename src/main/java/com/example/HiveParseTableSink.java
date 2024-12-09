package com.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveParseTableSink implements SinkFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(HiveParseTableSink.class);
    private static final String HIVE_URL = "jdbc:hive2://10.14.50.135:7001/ods_power";
    private static final String HIVE_USER = "hadoop";
    private static final String HIVE_PASSWORD = "";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    static {
        try {
            // 加载多个可能的Hive驱动
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
            } catch (ClassNotFoundException e) {
                LOG.warn("Failed to load HiveDriver, trying alternative driver");
                try {
                    Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
                } catch (ClassNotFoundException e2) {
                    LOG.error("Failed to load both Hive drivers", e2);
                    throw e2;
                }
            }
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to load Hive driver", e);
            throw new RuntimeException("No Hive driver found", e);
        }
    }

    @Override
    public void invoke(String value, Context context) {
        try {
            // 解析CDC JSON数据
            JSONObject jsonObject = new JSONObject(value);
            String op = jsonObject.optString("op", "");
            
            // 只处理写操作（c-创建，u-更新，d-删除），跳过读操作（r）
            if ("r".equals(op)) {
                LOG.debug("Skipping read operation");
                return;
            }

            LOG.info("Received CDC event: op={}, data={}", op, value);
            
            String ts = jsonObject.optString("ts_ms", "");
            String id = jsonObject.optString("id", "");
            String data = jsonObject.has("after") ? 
                         jsonObject.getJSONObject("after").toString() : 
                         jsonObject.getJSONObject("before").toString();
            
            // 获取分区日期，格式为yyyyMMdd
            String pt_dt = LocalDate.now().format(DATE_FORMATTER);
            
            LOG.info("Preparing to insert: op={}, ts={}, id={}, pt_dt={}", op, ts, id, pt_dt);
            
            // 连接Hive并插入数据
            try (Connection conn = DriverManager.getConnection(HIVE_URL, HIVE_USER, HIVE_PASSWORD);
                 Statement stmt = conn.createStatement()) {
                
                LOG.info("Connected to Hive successfully");
                
                // 1. 禁用自动统计信息收集
                stmt.execute("SET hive.stats.autogather=false");
                
                // 2. 如果分区不存在，先创建分区
                String addPartition = String.format(
                    "ALTER TABLE ods_power.rent_new_agree_tb_parse ADD IF NOT EXISTS PARTITION (pt_dt='%s')",
                    pt_dt
                );
                stmt.execute(addPartition);
                
                // 3. 使用直接插入语句
                String insertSQL = String.format(
                    "INSERT INTO TABLE ods_power.rent_new_agree_tb_parse PARTITION (pt_dt='%s') " +
                    "VALUES ('%s', '%s', '%s', '%s')",
                    pt_dt,
                    id.replace("'", "\\'"),
                    op.replace("'", "\\'"),
                    ts.replace("'", "\\'"),
                    data.replace("'", "\\'")
                );
                
                LOG.info("Executing SQL: {}", insertSQL);
                stmt.execute(insertSQL);
                LOG.info("Data inserted successfully");
            }
        } catch (Exception e) {
            LOG.error("Error processing CDC event", e);
            e.printStackTrace();
        }
    }
} 