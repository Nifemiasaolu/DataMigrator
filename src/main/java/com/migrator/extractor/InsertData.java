package com.migrator.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InsertData {
    private static final Logger logger = LoggerFactory.getLogger(InsertData.class);

    public static void insertData(Connection conn, String table, List<Map<String, Object>> rows) throws SQLException {
        if (rows == null || rows.isEmpty()) {
            logger.warn(" ⚠️ No data provided for insertion into table: {}", table);
            return;
        }

        Map<String, Object> firstRow = rows.get(0);
        List<String> columnsList = new ArrayList<>(firstRow.keySet());
        String columns = String.join(", ", columnsList);
        String placeholders = String.join(", ", Collections.nCopies(columnsList.size(), "?"));
        String sql = String.format("INSERT INTO %s (%s) VALUES (%S) ON CONFLICT DO NOTHING", table, columns, placeholders);

        conn.setAutoCommit(false);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Map<String, Object> row : rows) {
                int index = 1;
                for (String col : columnsList) {
                    Object value = row.getOrDefault(col, null);
                    stmt.setObject(index++, value);
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
            conn.commit();
            logger.info("\n::: ✅ Inserted {} row(s) into [{}].:::", rows.size(), table);
        } catch (SQLException e) {
            conn.rollback();
            logger.error("\n::: ❌ Failed to insert into table [{}]: {} :::", table, e.getMessage(), e);
            throw e;
        }

    }
}
