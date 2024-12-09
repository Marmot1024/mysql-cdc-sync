package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HiveDataMerger {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDataMerger.class);
    
    private static final String HIVE_URL = "jdbc:hive2://10.14.50.135:7001/ods_power";
    private static final String HIVE_USER = "hadoop";
    private static final String HIVE_PASSWORD = "";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            LOG.error("Failed to load Hive driver", e);
            throw new RuntimeException("No Hive driver found", e);
        }
    }

    public static void mergeData(LocalDate partitionDate) {
        String pt_dt = partitionDate.format(DATE_FORMATTER);
        LOG.info("Starting merge process for partition pt_dt={} at {}", 
                pt_dt, LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        try (Connection conn = DriverManager.getConnection(HIVE_URL, HIVE_USER, HIVE_PASSWORD);
             Statement stmt = conn.createStatement()) {

            // 1. 禁用自动统计信息收集
            stmt.execute("SET hive.stats.autogather=false");

            // 2. 处理增量数据
            String mergeSQL = String.format(
                "INSERT OVERWRITE TABLE ods_power.rent_new_agree_tb PARTITION (pt_dt='%s') " +
                "SELECT t.* FROM (" +
                "  SELECT code, user_id, company_id, protocol_no, file_url, img_url, " +
                "         rent_no, rent_detail_no, agree_version, create_time, update_time, " +
                "         check_info, cross_battery_type_name, change_origin, system_change, " +
                "         battery_type_group_id " +
                "  FROM ods_power.rent_new_agree_tb WHERE pt_dt='%s' " +
                "  UNION ALL " +
                "  SELECT " +
                "    get_json_object(data, '$.code'), " +
                "    get_json_object(data, '$.user_id'), " +
                "    get_json_object(data, '$.company_id'), " +
                "    get_json_object(data, '$.protocol_no'), " +
                "    get_json_object(data, '$.file_url'), " +
                "    get_json_object(data, '$.img_url'), " +
                "    get_json_object(data, '$.rent_no'), " +
                "    get_json_object(data, '$.rent_detail_no'), " +
                "    get_json_object(data, '$.agree_version'), " +
                "    get_json_object(data, '$.create_time'), " +
                "    get_json_object(data, '$.update_time'), " +
                "    get_json_object(data, '$.check_info'), " +
                "    get_json_object(data, '$.cross_battery_type_name'), " +
                "    get_json_object(data, '$.change_origin'), " +
                "    get_json_object(data, '$.system_change'), " +
                "    get_json_object(data, '$.battery_type_group_id') " +
                "  FROM (" +
                "    SELECT * FROM ods_power.rent_new_agree_tb_parse " +
                "    WHERE pt_dt='%s' AND op != 'd' " +  // 当前分区
                "    UNION ALL " +
                "    SELECT * FROM ods_power.rent_new_agree_tb_parse " +
                "    WHERE pt_dt='%s' AND op != 'd' " +  // 前一天分区
                "  ) parse_data" +
                ") t GROUP BY t.code, t.user_id, t.company_id, t.protocol_no, t.file_url, " +
                "t.img_url, t.rent_no, t.rent_detail_no, t.agree_version, t.create_time, " +
                "t.update_time, t.check_info, t.cross_battery_type_name, t.change_origin, " +
                "t.system_change, t.battery_type_group_id",
                pt_dt, pt_dt, pt_dt, partitionDate.minusDays(1).format(DATE_FORMATTER)
            );

            LOG.info("Executing merge SQL for partition: {}, including data from previous day: {}", 
                    pt_dt, partitionDate.minusDays(1).format(DATE_FORMATTER));
            stmt.execute(mergeSQL);
            LOG.info("Merge completed successfully for partition: {} at {}", 
                    pt_dt, LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        } catch (Exception e) {
            LOG.error("Error during merge for partition: {}", pt_dt, e);
            e.printStackTrace();
        }
    }
} 