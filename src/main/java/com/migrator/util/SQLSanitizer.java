package com.migrator.util;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SQLSanitizer {

    private static final List<Pattern> MYSQL_TABLE_SPEC_PATTERNS = List.of(
            Pattern.compile("(?i)\\s*ENGINE=\\S+"),
            Pattern.compile("(?i)\\s*DEFAULT CHARSET=\\S+"),
            Pattern.compile("(?i)\\s*CHARACTER SET\\s+\\S+"),
            Pattern.compile("(?i)\\s*COLLATE=\\S+")
    );
    private static final Set<String> MYSQL_COLUMN_SPEC = Set.of(
            "AUTO_INCREMENT",
            "UNASSIGNED",
            "ZEROFILL"
    ).stream().map(String::toUpperCase).collect(Collectors.toSet());
    private static final Set<String> RESERVED_KEYWORDS = Set.of(
            "user", "select", "insert", "update", "delete", "where", "from",
            "function", "group", "order", "by", "table", "join", "left", "right",
            "date", "time", "timestamp", "interval", "limit", "offset", "primary", "key"
    );

    public static List<String> cleanColumnSpec(List<String> specs) {
        if (specs == null) return null;
        return specs.stream().filter(spec -> !MYSQL_COLUMN_SPEC.contains(spec.toUpperCase())).collect(Collectors.toList());
    }

    public static List<String> cleanDDLOptions(List<String> ddlList) {
        return ddlList.stream()
                .map(SQLSanitizer::removeDDLMetaData)
                .toList();
    }

    public static String removeDDLMetaData(String ddl) {
        String result = ddl;
        for (Pattern pattern : MYSQL_TABLE_SPEC_PATTERNS) {
            result = pattern.matcher(result).replaceAll("");
        }
        return result.trim();
    }

    public static String removeBackticks(String ddl) {
        return ddl.replace("`", "");
    }

    public static String removeTrailingCommas(String ddl) {
        return ddl.replaceAll(",\\s*\\)", ")");
    }

    public static String cleanMySQLDDLForPostgres(String ddl) {
        String cleaned = removeBackticks(ddl);
        cleaned = cleanDDLOptions(Collections.singletonList(cleaned)).get(0);
        cleaned = removeTrailingCommas(cleaned);
        return cleaned.trim().replaceAll(" +", " ");
    }

    public static String sanitizeColumnNamesInDDL(String ddl) {
        StringBuilder sanitized = new StringBuilder();
        String[] lines = ddl.split("\n");

        for (String line : lines) {
            String trimmed = line.trim().toLowerCase();
            if (trimmed.startsWith("primary key") ||
                    trimmed.startsWith("foreign key") ||
                    trimmed.startsWith("unique") ||
                    trimmed.startsWith("constraint") ||
                    trimmed.startsWith("key") ||
                    trimmed.startsWith("check")) {
                sanitized.append(line).append("\n");
                continue;
            }

            Matcher matcher = Pattern.compile("^\\s*`?(\\w+)`?\\s+").matcher(line);
            if (matcher.find()) {
                String columnName = matcher.group(1);
                String quotedName = RESERVED_KEYWORDS.contains(columnName.toLowerCase())
                        ? "\"" + columnName + "\""
                        : columnName;

                line = line.replaceFirst(columnName, quotedName);
            }

            sanitized.append(line).append("\n");
        }

        return sanitized.toString();
    }
}
