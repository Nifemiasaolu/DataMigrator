DataMigrator is a Java-based utility designed to migrate data and schemas from a MySQL database to a PostgreSQL
database.
It uses multithreading to handle large datasets efficiently converts MySQL DDL to be PostgreSQL-compatible, using
JSQLParser.

### 📌 **Features**

✅ Automatic schema extraction from MySQL and recreation in PostgreSQL

✅ Data migration in batches using multi-threaded workers

✅ Type mapping from MySQL to PostgreSQL

✅ Sanitization of MySQL-specific DDL elements (e.g., AUTO_INCREMENT, ENGINE, CHARSET, etc.)

✅ Configurable connection management

### ⚙️ **Configuration**

Update your database configuration in `config.properties` file, as well as username and password.

// Example

`MYSQL_URL=jdbc:mysql://localhost:3306/source_db`
`POSTGRE_URL=jdbc:postgresql://localhost:5432/target_db`

Ensure that the database URLs and credentials are correct before running the migration.

### 🚀 **How to Run**

=> Build the maven project

`mvn clean install`

=> Run on Main.java file (Entry Point).