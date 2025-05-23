package com.migrator.extractor;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FetchData {

    public static List<Map<String, Object>> fetchData(Connection conn, String table, int offset, int batchSize) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        String query = "SELECT * FROM " + table + " LIMIT ? OFFSET ?";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setInt(1, batchSize);
            stmt.setInt(2, offset);

            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(meta.getColumnName(i), rs.getObject(i));
                    }
                    rows.add(row);
                }

            }
        }
        return rows;
    }
}
