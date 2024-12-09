package com.example.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQLDDLExtractor {
    static {
        try {
            // 加载Hive JDBC驱动
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            // 加载MySQL JDBC驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static final String MYSQL_URL = "jdbc:mysql://10.12.55.40:40000/cos_power_order_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    private static final String MYSQL_USER = "dev_dbo_test";
    private static final String MYSQL_PASSWORD = "B4DDC0B232D0E997";
    
    private static final String HIVE_URL = "jdbc:hive2://10.14.50.135:7001/ods_power";
    private static final String HIVE_USER = "hadoop";
    private static final String HIVE_PASSWORD = "";

    public static void createHiveTables(String database, String table) {
        // 先从MySQL获取表结构
        List<String> columnDefinitions = new ArrayList<>();
        try (Connection mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            DatabaseMetaData metaData = mysqlConn.getMetaData();
            ResultSet columns = metaData.getColumns(database, null, table, null);
            
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                columnDefinitions.add("`" + columnName + "` STRING");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        // 在Hive中创建表
        try (Connection hiveConn = DriverManager.getConnection(HIVE_URL, HIVE_USER, HIVE_PASSWORD)) {
            Statement stmt = hiveConn.createStatement();
            
            // 创建Parse表
            String parseDDL = String.format(
                "CREATE TABLE IF NOT EXISTS ods_power.%s_parse (\n" +
                "    `id` STRING,\n" +
                "    `op` STRING,\n" +
                "    `ts` STRING,\n" +
                "    `data` STRING\n" +
                ") PARTITIONED BY (pt_dt STRING)\n" +
                "STORED AS ORC\n" +
                "TBLPROPERTIES ('orc.compress'='SNAPPY')",
                table
            );
            stmt.execute(parseDDL);
            System.out.println("Parse表创建成功");

            // 创建ODS表
            String odsDDL = String.format(
                "CREATE TABLE IF NOT EXISTS ods_power.%s (\n%s\n)\n" +
                "PARTITIONED BY (pt_dt STRING)\n" +
                "STORED AS ORC\n" +
                "TBLPROPERTIES ('orc.compress'='SNAPPY')",
                table,
                String.join(",\n", columnDefinitions)
            );
            stmt.execute(odsDDL);
            System.out.println("ODS表创建成功");
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 这个方法用于测试和预览DDL
    public static String getTableDDL(String database, String table) {
        StringBuilder ddl = new StringBuilder();
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            // 获取表结构信息
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(database, null, table, null);
            
            List<String> columnDefinitions = new ArrayList<>();
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                // Hive表所有字段都使用STRING类型
                columnDefinitions.add("`" + columnName + "` STRING");
            }

            // 构建Parse表DDL
            String parseDDL = String.format(
                "CREATE TABLE IF NOT EXISTS ods_power.%s_parse (\n" +
                "    `id` STRING,\n" +
                "    `op` STRING,\n" +
                "    `ts` STRING,\n" +
                "    `data` STRING\n" +
                ") PARTITIONED BY (pt_dt STRING)\n" +
                "STORED AS ORC\n" +
                "TBLPROPERTIES ('orc.compress'='SNAPPY')",
                table
            );

            // 构建ODS表DDL
            String odsDDL = String.format(
                "CREATE TABLE IF NOT EXISTS ods_power.%s (\n%s\n)\n" +
                "PARTITIONED BY (pt_dt STRING)\n" +
                "STORED AS ORC\n" +
                "TBLPROPERTIES ('orc.compress'='SNAPPY')",
                table,
                String.join(",\n", columnDefinitions)
            );

            ddl.append("-- Parse表DDL:\n").append(parseDDL).append(";\n\n");
            ddl.append("-- ODS表DDL:\n").append(odsDDL).append(";");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ddl.toString();
    }
} 