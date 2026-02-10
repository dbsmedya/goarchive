// MySQL Shell JavaScript script to reset source database
// Drops and recreates the database from Sakila SQL files.
//
// Can be run with existing connection or will connect using environment variables.
//
// Environment variables:
//   TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD, TEST_SOURCE_DB
//   SAKILA_DIR

var sourceHost = os.getenv("TEST_SOURCE_HOST") || "127.0.0.1";
var sourcePort = os.getenv("TEST_SOURCE_PORT") || "3305";
var sourceUser = os.getenv("TEST_SOURCE_USER") || "root";
var sourcePass = os.getenv("TEST_SOURCE_PASSWORD") || os.getenv("MYSQL_ROOT_PASSWORD") || "";
var sourceDb = os.getenv("TEST_SOURCE_DB") || "db1";

var sakilaDir = os.getenv("SAKILA_DIR") || "./sakila-db";
var schemaFile = sakilaDir + "/sakila-schema.sql";
var dataFile = sakilaDir + "/sakila-data.sql";

print("============================================================");
print("Resetting Source Database");
print("============================================================");
print("Source DB:    " + sourceHost + ":" + sourcePort + "/" + sourceDb);
print("Sakila Dir:   " + sakilaDir);
print("------------------------------------------------------------");

// Check if Sakila files exist
if (!os.path.isfile(schemaFile)) {
    print("ERROR: Sakila schema file not found: " + schemaFile);
    exit(1);
}
if (!os.path.isfile(dataFile)) {
    print("ERROR: Sakila data file not found: " + dataFile);
    exit(1);
}

// Connect if not already connected
if (!session || !session.isOpen()) {
    print("Connecting to source database...");
    var connStr = sourceUser + "@" + sourceHost + ":" + sourcePort;
    if (sourcePass) {
        connStr = sourceUser + ":" + sourcePass + "@" + sourceHost + ":" + sourcePort;
    }
    try {
        shell.connect(connStr);
    } catch (err) {
        print("ERROR: Failed to connect to source database: " + err.message);
        exit(1);
    }
} else {
    print("Using existing database connection...");
}

// Helper function to execute SQL that works with both X Protocol and Classic sessions
function runSQL(query) {
    if (session.runSql) {
        // ClassicSession
        return session.runSql(query);
    } else if (session.sql) {
        // X Protocol Session
        return session.sql(query).execute();
    } else {
        throw new Error("Unknown session type - no sql or runSql method available");
    }
}

// Drop database if exists
print("Dropping database '" + sourceDb + "' if exists...");
try {
    runSQL("DROP DATABASE IF EXISTS `" + sourceDb + "`");
    print("Database dropped.");
} catch (err) {
    print("WARNING: Could not drop database: " + err.message);
}

// Recreate database
print("Creating database '" + sourceDb + "'...");
runSQL("CREATE DATABASE `" + sourceDb + "`");
runSQL("USE `" + sourceDb + "`");

print("------------------------------------------------------------");
print("Source database reset complete!");
print("NOTE: Schema and data must be loaded separately using SQL mode");
print("============================================================");

session.close();
