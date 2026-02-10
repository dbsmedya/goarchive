// MySQL Shell JavaScript script to load Sakila database into source (master) database
//
// Can be run with existing connection or will connect using environment variables.
//
// Environment variables:
//   TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD, TEST_SOURCE_DB
//   SAKILA_DIR (default: ./sakila-db)

// Read configuration from environment variables
var sourceHost = os.getenv("TEST_SOURCE_HOST") || "127.0.0.1";
var sourcePort = os.getenv("TEST_SOURCE_PORT") || "3305";
var sourceUser = os.getenv("TEST_SOURCE_USER") || "root";
var sourcePass = os.getenv("TEST_SOURCE_PASSWORD") || os.getenv("MYSQL_ROOT_PASSWORD") || "";
var sourceDb = os.getenv("TEST_SOURCE_DB") || "db1";

// Determine Sakila directory - use env var (must be exported) or default to current dir
var sakilaDir = os.getenv("SAKILA_DIR");
if (!sakilaDir || sakilaDir === "") {
    sakilaDir = "./sakila-db";
}

var schemaFile = sakilaDir + "/sakila-schema.sql";
var dataFile = sakilaDir + "/sakila-data.sql";

print("============================================================");
print("Loading Sakila Database into Source (Master)");
print("============================================================");
print("Source DB:    " + sourceHost + ":" + sourcePort + "/" + sourceDb);
print("Sakila Dir:   " + sakilaDir);
print("------------------------------------------------------------");

// Check if Sakila files exist
if (!os.path.isfile(schemaFile)) {
    print("ERROR: Sakila schema file not found: " + schemaFile);
    print("Run get_sakila_db.sh first to download the Sakila database.");
    exit(1);
}
if (!os.path.isfile(dataFile)) {
    print("ERROR: Sakila data file not found: " + dataFile);
    print("Run get_sakila_db.sh first to download the Sakila database.");
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

// Create database if not exists and switch to it
print("Creating database '" + sourceDb + "' if not exists...");
runSQL("CREATE DATABASE IF NOT EXISTS `" + sourceDb + "`");
runSQL("USE `" + sourceDb + "`");

// Load schema
print("------------------------------------------------------------");
print("Loading Sakila schema from: " + schemaFile);
print("------------------------------------------------------------");
try {
    shell.loadSource(schemaFile);
    print("Schema loaded successfully!");
} catch (err) {
    print("ERROR: Failed to load schema: " + err.message);
    session.close();
    exit(1);
}

// Load data
print("------------------------------------------------------------");
print("Loading Sakila data from: " + dataFile);
print("------------------------------------------------------------");
try {
    shell.loadSource(dataFile);
    print("Data loaded successfully!");
} catch (err) {
    print("ERROR: Failed to load data: " + err.message);
    session.close();
    exit(1);
}

// Show loaded tables
print("\n------------------------------------------------------------");
print("Tables in " + sourceDb + ":");
var tables = [];
if (session.runSql) {
    // ClassicSession - returns result directly
    tables = runSQL("SHOW TABLES").fetchAll();
} else {
    // X Protocol
    tables = session.sql("SHOW TABLES").execute().fetchAll();
}
if (tables.length === 0) {
    print("  (no tables found)");
} else {
    for (var i = 0; i < tables.length; i++) {
        print("  - " + tables[i][0]);
    }
}

// Show table metadata summary
print("\n------------------------------------------------------------");
print("Table Metadata Summary:");
var metadataQuery = "SELECT TABLE_NAME, ENGINE, TABLE_ROWS, " +
    "ROUND(DATA_LENGTH/1024/1024,2) as DATA_MB " +
    "FROM INFORMATION_SCHEMA.TABLES " +
    "WHERE TABLE_SCHEMA = '" + sourceDb + "' ORDER BY TABLE_NAME";

var metadataRows = [];
if (session.runSql) {
    metadataRows = runSQL(metadataQuery).fetchAll();
} else {
    metadataRows = session.sql(
        "SELECT TABLE_NAME, ENGINE, TABLE_ROWS, " +
        "ROUND(DATA_LENGTH/1024/1024,2) as DATA_MB " +
        "FROM INFORMATION_SCHEMA.TABLES " +
        "WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME"
    ).bind(sourceDb).execute().fetchAll();
}

if (metadataRows.length === 0) {
    print("  (no metadata found)");
} else {
    print("  TABLE_NAME           ENGINE    ROWS    DATA_MB");
    print("  ------------------   ------    ----    -------");
    for (var j = 0; j < metadataRows.length; j++) {
        var row = metadataRows[j];
        var name = row[0].padEnd(18);
        var engine = row[1].padEnd(8);
        var rows = String(row[2]).padStart(4);
        var dataMb = String(row[3]).padStart(7);
        print("  " + name + " " + engine + " " + rows + " " + dataMb);
    }
}

session.close();
print("\n============================================================");
print("Sakila database setup complete in '" + sourceDb + "'!");
print("============================================================");
