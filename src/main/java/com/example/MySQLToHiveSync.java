package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MySQLToHiveSync {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLToHiveSync.class);
    
    // MySQL连接配置
    private static final String MYSQL_URL = "jdbc:mysql://10.12.55.40:40000/cos_power_order_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "dev_dbo_test";
    private static final String MYSQL_PASSWORD = "B4DDC0B232D0E997";
    
    // Hive连接配置
    private static final String HIVE_URL = "jdbc:hive2://10.14.50.135:7001/ods_power";
    private static final String HIVE_USER = "hadoop";
    private static final String HIVE_PASSWORD = "";

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
            // 加载MySQL驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to load JDBC driver", e);
            throw new RuntimeException("No JDBC driver found", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Starting MySQL to Hive full sync job...");
        String pt_dt = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        
        try {
            // 1. 从MySQL读取数据
            List<String> rows = new ArrayList<>();
            try (Connection mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                 Statement stmt = mysqlConn.createStatement()) {
                
                LOG.info("Connected to MySQL successfully");
                ResultSet rs = stmt.executeQuery("SELECT * FROM rent_new_agree_tb");
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                // 获取列名
                List<String> columnNames = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    columnNames.add(metaData.getColumnName(i));
                }
                LOG.info("Table columns: {}", String.join(", ", columnNames));
                
                // 读取数据
                int rowCount = 0;
                while (rs.next()) {
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String value = rs.getString(i);
                        values.add(value == null ? "" : value.replace("'", "\\'"));
                    }
                    rows.add("('" + String.join("','", values) + "')");
                    rowCount++;
                    if (rowCount % 1000 == 0) {
                        LOG.info("Read {} rows from MySQL", rowCount);
                    }
                }
                LOG.info("Total read {} rows from MySQL", rowCount);
            }

            // 2. 写入Hive
            try (Connection hiveConn = DriverManager.getConnection(HIVE_URL, HIVE_USER, HIVE_PASSWORD);
                 Statement stmt = hiveConn.createStatement()) {
                
                LOG.info("Connected to Hive successfully");
                
                // 2.1 禁用自动统计信息收集
                stmt.execute("SET hive.stats.autogather=false");
                
                // 2.2 创建分区
                String addPartition = String.format(
                    "ALTER TABLE ods_power.rent_new_agree_tb ADD IF NOT EXISTS PARTITION (pt_dt='%s')",
                    pt_dt
                );
                stmt.execute(addPartition);
                LOG.info("Created partition pt_dt={}", pt_dt);

                // 2.3 分批插入数据
                int batchSize = 1000;
                for (int i = 0; i < rows.size(); i += batchSize) {
                    int endIndex = Math.min(i + batchSize, rows.size());
                    List<String> batch = rows.subList(i, endIndex);
                    
                    String insertSQL = String.format(
                        "INSERT INTO TABLE ods_power.rent_new_agree_tb PARTITION (pt_dt='%s') VALUES %s",
                        pt_dt,
                        String.join(",", batch)
                    );
                    
                    stmt.execute(insertSQL);
                    LOG.info("Inserted batch {} - {} into Hive", i, endIndex);
                }
            }
            
            LOG.info("Full sync completed successfully");
            
        } catch (Exception e) {
            LOG.error("Error during sync", e);
            e.printStackTrace();
        }
    }
} 