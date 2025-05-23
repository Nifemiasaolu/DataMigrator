package com.migrator.util;

import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class ConfigManager {
    private static final Properties props = new Properties();

    static {
        try (InputStream io = ConfigManager.class.getClassLoader().getResourceAsStream("config.properties")) {
            props.load(io);
            System.out.println(" ::: ✅ Successfully loaded config properties :::");
        } catch (Exception e) {
            throw new RuntimeException(String.format("::: Failed to load config: %s :::", e.getMessage()), e);
        }
    }

    public static String getProperty(String key) {
        return props.getProperty(key);
    }

    public static Connection getMYSQLConnection() throws SQLException {
        return DriverManager.getConnection(getProperty("MYSQL_URL"), getProperty("MYSQL_USER"), getProperty("MYSQL_PASSWORD"));
    }

    public static Connection getPostgreSQLConnection() throws SQLException {
        return DriverManager.getConnection(getProperty("POSTGRES_URL"), getProperty("POSTGRES_USER"), getProperty("POSTGRES_PASSWORD"));
    }
}
