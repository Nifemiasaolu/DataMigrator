package com.migrator;

import com.migrator.extractor.CreateTableSchema;
import com.migrator.transfer.DataMigrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        DataMigrator migrator = new DataMigrator();
        try {
            migrator.migrateAllTables();

            List<String> skippedTables = CreateTableSchema.getSkippedTables();
            if (!skippedTables.isEmpty()) {
                logger.warn("::: ⚠️ Skipped Tables (due to missing PK/UNIQUE constraints): :::");
                for (String table : skippedTables) {
                    logger.warn("::: Skipped Table: {} :::", table);
                }
            } else {
                logger.info("::: ✅ No tables were skipped. All eligible tables migrated. :::");
            }

            CreateTableSchema.printSummary();
            logger.info("::: ✅ Migration Completed! :::");
        } catch (Exception e) {
            logger.error("::: ❌ Error occurred while migrating data: ", e);
        }
    }
}