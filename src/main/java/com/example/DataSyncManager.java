package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DataSyncManager {
    private static final Logger LOG = LoggerFactory.getLogger(DataSyncManager.class);
    private static final String FIRST_RUN_FLAG = "first_run.flag";

    public static void main(String[] args) {
        LOG.info("====================================");
        LOG.info("Starting Data Sync Manager...");
        LOG.info("====================================");

        try {
            // 1. 检查是否首次运行
            LOG.info("Checking first run status...");
            if (isFirstRun()) {
                LOG.info("First time run detected");
                LOG.info("Starting full sync process...");
                try {
                    MySQLToHiveSync.main(null);
                    markFirstRunComplete();
                    LOG.info("Full sync completed successfully");
                } catch (Exception e) {
                    LOG.error("Failed to complete full sync", e);
                    System.exit(1);
                }
            } else {
                LOG.info("Not first run, skipping full sync");
            }

            // 2. 启动增量采集
            LOG.info("Initializing CDC sync...");
            CompletableFuture<Void> cdcFuture = CompletableFuture.runAsync(() -> {
                try {
                    LOG.info("Starting CDC sync process");
                    MySQLCDCSync.main(null);
                } catch (Exception e) {
                    LOG.error("CDC sync process failed", e);
                }
            });

            // 3. 启动定时合并任务
            LOG.info("Initializing merge scheduler...");
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            
            // 打印当前时间和下次执行时间
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime nextRun = now.plusHours(1).withMinute(0).withSecond(0);
            LOG.info("Current time: {}", now);
            LOG.info("Next merge scheduled at: {}", nextRun);

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    LOG.info("====================================");
                    LOG.info("Starting scheduled merge task at: {}", LocalDateTime.now());
                    LocalDateTime currentTime = LocalDateTime.now();
                    LocalDate currentDay = currentTime.toLocalDate();
                    
                    if (currentTime.getHour() == 0) {
                        LocalDate previousDay = currentDay.minusDays(1);
                        LOG.info("Processing previous day's data: {}", previousDay);
                        HiveDataMerger.mergeData(previousDay);
                    }
                    
                    LOG.info("Processing current day's data: {}", currentDay);
                    HiveDataMerger.mergeData(currentDay);
                    LOG.info("Merge task completed at: {}", LocalDateTime.now());
                    LOG.info("====================================");
                } catch (Exception e) {
                    LOG.error("Merge task failed", e);
                }
            }, 0, 1, TimeUnit.HOURS);

            LOG.info("====================================");
            LOG.info("All components initialized successfully");
            LOG.info("- CDC sync: Running");
            LOG.info("- Hourly merge: Scheduled");
            LOG.info("====================================");

            // 等待CDC完成（实际上不会完成，除非发生错误）
            cdcFuture.get();

        } catch (Exception e) {
            LOG.error("Critical error in Data Sync Manager", e);
            System.exit(1);
        }
    }

    private static boolean isFirstRun() {
        return !new java.io.File(FIRST_RUN_FLAG).exists();
    }

    private static void markFirstRunComplete() throws Exception {
        new java.io.File(FIRST_RUN_FLAG).createNewFile();
    }
} 