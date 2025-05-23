package com.migrator.extractor;

import com.migrator.util.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;


public class CreateTableSchema {
    private static final Logger logger = LoggerFactory.getLogger(CreateTableSchema.class);

    private static final List<String> skippedTables = new ArrayList<>();
    private static final List<String> migratedTables = new ArrayList<>();
    private static final List<String> failedToParseTables = new ArrayList<>();

    public static void createTableSchema(Connection mysqlConn, Connection postgresConn, String table) {
        final TypeMapper mapper = new TypeMapper();

        logger.info("::: 🔄 Processing table: [{}] :::", table);

        try (Statement stmt = mysqlConn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + table)) {

            if (!rs.next()) {
                logger.warn("::: ⚠️ No CREATE TABLE result found for [{}]. Possibly a VIEW or lacks privileges. :::", table);
                return;
            }

            String mySqlDDL = rs.getString(2);
            String cleanedUpDDL = SQLSanitizer.cleanMySQLDDLForPostgres(mySqlDDL);
            String sanitizedDDL = SQLSanitizer.sanitizeColumnNamesInDDL(cleanedUpDDL);

            CreateTable createTable;
            try {
                createTable = (CreateTable) CCJSqlParserUtil.parse(sanitizedDDL);
            } catch (Exception parseEx) {
                failedToParseTables.add(table);
                logger.error("❌ Failed to parse DDL for [{}]: {}", table, parseEx.getMessage());
                return;
            }

            // Transform data types and clean column specs
            List<ColumnDefinition> columns = createTable.getColumnDefinitions();
            for (ColumnDefinition column : columns) {
                String mysqlType = column.getColDataType().getDataType();
                column.getColDataType().setDataType(mapper.map(mysqlType));

                if (column.getColumnSpecs() != null) {
                    List<String> cleanedSpecs = SQLSanitizer.cleanColumnSpec(column.getColumnSpecs());
                    column.setColumnSpecs(cleanedSpecs);
                }
            }

            // Ensure table has PK or UNIQUE Constraint
            List<Index> indexes = createTable.getIndexes();
            boolean hasPrimaryOrUniqueKey = indexes != null && indexes.stream().anyMatch(
                    index -> "PRIMARY KEY".equalsIgnoreCase(index.getType()) || "UNIQUE".equalsIgnoreCase(index.getType()));

            if (!hasPrimaryOrUniqueKey) {
                skippedTables.add(table);
                logger.warn("\n::: ⚠️ Skipping table [{}] — No PRIMARY or UNIQUE KEY found.:::", table);
                return;
            }


            createTable.setTable(new Table(table));
            try (Statement pgStmt = postgresConn.createStatement()) {
                pgStmt.execute(createTable.toString());
                postgresConn.commit();
                migratedTables.add(table);
                logger.info("\n::: ✅ Table [{}] successfully created in Postgres :::", table);
            } catch (SQLException e) {
                postgresConn.rollback();
                if (e.getMessage().toLowerCase().contains("already exists")) {
                    logger.warn(" ::: ⚠️ Table [{}] already exists. Skipping creation. :::", table);

                } else {
                    throw e;
                }
            }

        } catch (Exception e) {
            logger.error("::: ❌ Failed to create schema for table [{}]: {} :::", table, e.getMessage(), e);
            try {
                postgresConn.rollback();
            } catch (SQLException ex) {
                logger.error("Failed to rollback transaction: {}", ex.getMessage(), ex);
            }
        }
    }

    public static List<String> getSkippedTables() {
        return skippedTables;
    }

    public static List<String> getMigratedTables() {
        return migratedTables;
    }

    public static List<String> getFailedToParseTables() {
        return failedToParseTables;
    }

    public static void printSummary() {
        logger.info("\n===== 🧾 Migration Summary =====");
        logger.info("::: ✅ Migrated Tables: {} :::", migratedTables.size());
        logger.info("::: ⚠️ Skipped (No PK/Unique): {} :::", skippedTables.size());
        logger.info("::: ❌ Failed to Parse: {} :::", failedToParseTables.size());
        logger.info("::: 🟡 Skipped Tables: {} :::", skippedTables);
        logger.info("::: 🔴 Failed to Parse: {} :::", failedToParseTables);
        logger.info("=================================\n");
    }
}
