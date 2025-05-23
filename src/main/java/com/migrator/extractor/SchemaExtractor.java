package com.migrator.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class SchemaExtractor {
    private static final Logger logger = LoggerFactory.getLogger(SchemaExtractor.class);
    private final Connection conn;

    public SchemaExtractor(Connection conn) {
        this.conn = conn;
    }

    public String getCreateTableDDL(String tableName) throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE" + tableName);
        if (rs.next()) {
            return rs.getString(2);
        }
        throw new Exception("Could not get DDL for table: " + tableName);
    }

    public List<String> getAllTableNames(String tableName) throws Exception {
        List<String> tables = new ArrayList<>();
        ResultSet rs = conn.getMetaData().getTables(tableName, null, "%", new String[]{"TABLE"});

        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        logger.info("\n::: ✅ Table names successfully gotten from db :::");
        return tables;
    }

    public int getTotalRowCount(String tableName) throws SQLException {
        String countQuery = "SELECT COUNT(*) FROM " + tableName;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }


}
