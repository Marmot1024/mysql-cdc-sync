package com.example;

import com.example.utils.MySQLDDLExtractor;

public class DDLTest {
    public static void main(String[] args) {
        try {
            // 先测试DDL生成
            String ddl = MySQLDDLExtractor.getTableDDL("cos_power_order_db", "rent_new_agree_tb");
            System.out.println("Generated DDL:");
            System.out.println(ddl);
            
            // 如果DDL生成正确，再测试建表
            System.out.println("\nTrying to create tables in Hive...");
            MySQLDDLExtractor.createHiveTables("cos_power_order_db", "rent_new_agree_tb");
            
        } catch (Exception e) {
            System.err.println("Error occurred:");
            e.printStackTrace();
        }
    }
} 