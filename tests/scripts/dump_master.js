// MySQL Shell JavaScript script to dump schemas only from source database
//
// Can be run with existing connection or will connect using environment variables.
//
// Environment variables:
//   TEST_SOURCE_HOST, TEST_SOURCE_PORT, TEST_SOURCE_USER, TEST_SOURCE_PASSWORD, TEST_SOURCE_DB
//   DUMP_DIR

// Read configuration from environment variables
var sourceHost = os.getenv("TEST_SOURCE_HOST") || "127.0.0.1";
var sourcePort = os.getenv("TEST_SOURCE_PORT") || "3305";
var sourceUser = os.getenv("TEST_SOURCE_USER") || "root";
var sourcePass = os.getenv("TEST_SOURCE_PASSWORD") || os.getenv("MYSQL_ROOT_PASSWORD") || "";
var sourceDb = os.getenv("TEST_SOURCE_DB") || "db1";
var dumpDir = os.getenv("DUMP_DIR") || "/tmp/db1_schema_dump";

print("============================================================");
print("Dumping schema from source database");
print("============================================================");
print("Source:    " + sourceHost + ":" + sourcePort + "/" + sourceDb);
print("Output:    " + dumpDir);
print("------------------------------------------------------------");

var dumpOptions = {
    threads: 4,
    ddlOnly: true,
    showProgress: true,
    triggers: false,
    routines: false,
    events: false
};

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

print("Starting schema dump...");
print("------------------------------------------------------------");

try {
    util.dumpSchemas([sourceDb], dumpDir, dumpOptions);
    print("------------------------------------------------------------");
    print("Schema dump completed successfully!");
    print("Output location: " + dumpDir);
} catch (err) {
    print("ERROR: Schema dump failed: " + err.message);
    session.close();
    exit(1);
}

session.close();
