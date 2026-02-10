// MySQL Shell JavaScript script to load dumped schemas into archive database
//
// Can be run with existing connection or will connect using environment variables.
//
// Environment variables:
//   TEST_ARCHIVE_HOST, TEST_ARCHIVE_PORT, TEST_ARCHIVE_USER, TEST_ARCHIVE_PASSWORD, TEST_ARCHIVE_DB
//   DUMP_DIR

// Read configuration from environment variables
var archiveHost = os.getenv("TEST_ARCHIVE_HOST") || "127.0.0.1";
var archivePort = os.getenv("TEST_ARCHIVE_PORT") || "3307";
var archiveUser = os.getenv("TEST_ARCHIVE_USER") || "root";
var archivePass = os.getenv("TEST_ARCHIVE_PASSWORD") || os.getenv("MYSQL_ROOT_PASSWORD") || "";
var archiveDb = os.getenv("TEST_ARCHIVE_DB") || "sakila_archive";
var dumpDir = os.getenv("DUMP_DIR") || "/tmp/db1_schema_dump";

print("============================================================");
print("Loading schema dump into archive database");
print("============================================================");
print("Dump directory: " + dumpDir);
print("Archive DB:     " + archiveDb);
print("Archive Host:   " + archiveHost + ":" + archivePort);
print("------------------------------------------------------------");

// Load options
var loadOptions = {
    threads: 4,
    schema: archiveDb,
    showProgress: true
};

// Connect if not already connected
if (!session || !session.isOpen()) {
    print("Connecting to archive database...");
    var connStr = archiveUser + "@" + archiveHost + ":" + archivePort;
    if (archivePass) {
        connStr = archiveUser + ":" + archivePass + "@" + archiveHost + ":" + archivePort;
    }
    try {
        shell.connect(connStr);
    } catch (err) {
        print("ERROR: Failed to connect to archive database: " + err.message);
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

// Create archive database if it doesn't exist
print("Creating archive database if not exists...");
runSQL("CREATE DATABASE IF NOT EXISTS `" + archiveDb + "`");

print("Loading schema from " + dumpDir + " into database " + archiveDb + "...");
print("------------------------------------------------------------");

try {
    util.loadDump(dumpDir, loadOptions);
    print("------------------------------------------------------------");
    print("Schema loaded successfully!");
} catch (err) {
    print("ERROR: Failed to load dump: " + err.message);
    session.close();
    exit(1);
}

// Verify tables were created
print("\nTables in " + archiveDb + ":");
var tables = [];
if (session.runSql) {
    tables = runSQL("SHOW TABLES FROM `" + archiveDb + "`").fetchAll();
} else {
    tables = session.sql("SHOW TABLES FROM `" + archiveDb + "`").execute().fetchAll();
}
if (tables.length === 0) {
    print("  (no tables found)");
} else {
    for (var i = 0; i < tables.length; i++) {
        print("  - " + tables[i][0]);
    }
}

session.close();
print("\nArchive schema setup complete!");
