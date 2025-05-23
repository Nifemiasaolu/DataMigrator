package com.migrator.util;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeMapper {
    private static final Map<String, String> map = new HashMap<>();
    private static final Pattern TYPE_WITH_PARAMS = Pattern.compile("^([a-zA-Z]+)\\s*\\(([^)]+)\\)$");

    static {
        map.put("int", "INTEGER");
        map.put("tinyint", "SMALLINT");
        map.put("smallint", "SMALLINT");
        map.put("mediumint", "INTEGER");
        map.put("bigint", "BIGINT");
        map.put("float", "REAL");
        map.put("double", "NUMERIC");
        map.put("decimal", "DECIMAL");
        map.put("char", "CHAR");
        map.put("varchar", "VARCHAR");
        map.put("text", "TEXT");
        map.put("mediumtext", "TEXT");
        map.put("longtext", "TEXT");
        map.put("date", "DATE");
        map.put("datetime", "TIMESTAMP");
        map.put("timestamp", "TIMESTAMP");
        map.put("time", "TIME");
        map.put("year", "INTEGER");
        map.put("bit", "BOOLEAN");
        map.put("boolean", "BOOLEAN");
        map.put("enum", "TEXT");
        map.put("blob", "BYTEA");
        map.put("longblob", "BYTEA");
    }

    public String map(String mysqlTypeRaw) {
        if (mysqlTypeRaw == null || mysqlTypeRaw.isEmpty()) return "VARCHAR";

        String mySqlType = mysqlTypeRaw.toLowerCase().trim();
        Matcher matcher = TYPE_WITH_PARAMS.matcher(mySqlType);

        if (matcher.matches()) {
            String baseType = matcher.group(1);
            String params = matcher.group(2);

            if (baseType.equals("decimal") || baseType.equals("double") || baseType.equals("float")) {
                return "NUMERIC (" + params + ")";
            }

            if (baseType.equals("varchar") || baseType.equals("char")) {
                return map.getOrDefault(baseType, "VARCHAR") + "(" + params + ")";
            }

            return map.getOrDefault(baseType, "VARCHAR");
        }

        if (mySqlType.equals("double") || mySqlType.equals("float")) {
            return "NUMERIC";
        }

        return map.getOrDefault(mySqlType, "VARCHAR");
    }
}
