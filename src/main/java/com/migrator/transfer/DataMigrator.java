package com.migrator.transfer;

import com.migrator.extractor.CreateTableSchema;
import com.migrator.extractor.FetchData;
import com.migrator.extractor.InsertData;
import com.migrator.extractor.SchemaExtractor;
import com.migrator.util.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class DataMigrator {
    private static final Logger logger = LoggerFactory.getLogger(DataMigrator.class);
    private static final int THREAD_COUNT = 4;
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_SIZE = 1000;

    private final ConcurrentHashMap<String, Boolean> createdTables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> tableLocks = new ConcurrentHashMap<>();

    public void migrateAllTables() {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        try (Connection mySqlConn = ConfigManager.getMYSQLConnection()) {

            SchemaExtractor extractor = new SchemaExtractor(mySqlConn);
            List<String> mySqlTablesNames = extractor.getAllTableNames("mybank");

            for (String table : mySqlTablesNames) {

                int totalRows = extractor.getTotalRowCount(table);
                int offset = 0;

                while (offset < totalRows) {
                    final String currentTable = table;
                    final int finalOffset = offset;

                    executor.submit(() -> {
                        // Initialize retry mechanism
                        int retries = 0;
                        boolean success = false;

                        try (Connection threadPostgresConn = ConfigManager.getPostgreSQLConnection()) {
                            threadPostgresConn.setAutoCommit(false);
                            ensureTableCreatedOnce(mySqlConn, threadPostgresConn, table);

                            while (!success && retries < MAX_RETRIES) {
                                try {
                                    List<Map<String, Object>> rows = FetchData.fetchData(mySqlConn, currentTable, finalOffset, BATCH_SIZE);
                                    logger.info("::: Rows data successfully fetched :::");
                                    InsertData.insertData(threadPostgresConn, currentTable, rows);
                                    success = true;
                                    logger.info("\n::: ✅ Successfully migrated [{}] rows from [{}] offset [{}] :::", rows.size(), currentTable, finalOffset);
                                } catch (Exception e) {
                                    threadPostgresConn.rollback();
                                    retries++;
                                    logger.error("\n::: ❌ Error migrating offset [{}] for table [{}], retry [{}] :::", finalOffset, currentTable, retries, e);

                                }
                            }
                        } catch (SQLException e) {
                            logger.error("\n::: Postgres thread connection error for table [{}] offset [{}]", currentTable, finalOffset, e);
                        }
                    });

                    offset += BATCH_SIZE;
                }

            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (Exception e) {
            logger.error("\n::: ❌ Fatal migration error: ", e);
        }
    }

    private void ensureTableCreatedOnce(Connection mySqlConn, Connection postgresConn, String tableName) throws SQLException {
        Object lock = tableLocks.computeIfAbsent(tableName, k -> new Object());

        synchronized (lock) {
            if (createdTables.containsKey(tableName)) return;

            try {
                CreateTableSchema.createTableSchema(mySqlConn, postgresConn, tableName);
                createdTables.put(tableName, true);
            } catch (Exception e) {
                logger.error("::: ❌ Failed to create table [{}] :::", tableName, e);
                throw new RuntimeException("Table creation failed for " + tableName, e);
            }
        }
    }

}
