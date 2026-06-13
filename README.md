# GoArchive - MySQL Batch Archiver, Copier & Purger

[![Go Version](https://img.shields.io/badge/Go-1.21+-blue)](https://golang.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange)](https://www.mysql.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

GoArchive is a Go-based CLI tool designed for archiving MySQL relational data across servers. It features automatic dependency resolution, crash recovery, replication lag monitoring, and a `copy-only` mode that never deletes source data.

## The Philosophy

Archiving is a custom-coded headache that developers end up building from scratch for every new project. Because every schema is different, "off-the-shelf" tools often fall short—they either ignore your foreign keys entirely or require a massive configuration just to avoid leaving behind a mess of orphaned records.

We got tired of reinventing the wheel and worrying about data integrity every time a table got too big. So we built GoArchive.

While legendary tools like pt-archiver are excellent for offloading single tables, they often fall short in complex ecosystems because they lack an inherent awareness of deep foreign key hierarchies. If you’ve ever looked at the MySQL Sakila sample database, you know that real-world relationships are rarely linear.

<img src="tests/sakila-EE.png" width="50%" alt="Sakila ERD Diagram">

GoArchive was born from the need to visualize and automate these complexities. However, to maintain the integrity of your production environment, we adhere to two core principles:

1. **Cold Data Only**
GoArchive is designed to move COLD data to an archive server—specifically for performance tuning or meeting GDPR compliance.

 > [!IMPORTANT] 
 > If you intend to archive "hot" data that is currently receiving heavy transactions, stop here. Grab a coffee, enjoy the sunshine, and reconsider your architecture. Live-data shifting is outside the scope of this tool.

2. **Zero-Impact Production Archiving**
  In high-traffic production environments, database locks are the enemy. GoArchive is built to be "invisible":

  Replication Friendly: Integrated monitoring ensures the tool pauses automatically if replica lag exceeds your thresholds.

  Intelligent Batching: We recognize that a single record in a master table (e.g., an Order) can represent millions of rows in child tables (e.g., Logs or Transitions).

  Asymmetric Processing: By processing in configurable batches, GoArchive completes the move-and-purge cycle without ever holding a long-term lock on the master table.

> [!NOTE]
> For the Faint-of-heart: it has a feature to match and compare the row counts or even SHA256 checksum of the records between archive and source before deletion.

---

## 🛑 Limitations & Constraints

To maintain high safety standards and predictable behavior, GoArchive currently operates with the following constraints. Please review these before integrating the tool into your workflow.

### 1. Supported Relationship Types

GoArchive currently supports **1:1** and **1:N** (One-to-Many) relationships.

* **Unsupported:** Many-to-Many (N:M) relationships and complex self-referential "Adjacency List" hierarchies (e.g., a table that references its own ID to create an infinite tree) are not currently handled by the automatic resolver.

### 2. Foreign Key `CASCADE`

The tool is designed to manage the deletion order manually via Kahn's Algorithm to prevent circular looping. If your schema relies heavily on `ON DELETE CASCADE` at the database level, the tool may encounter conflicts or redundant operations. We recommend using GoArchive on schemas where you want the **application** to control the deletion flow.

### 3. Database Triggers

GoArchive does not have any account for logic hidden within **MySQL Triggers**.

* If a `DELETE` on your source table fires a trigger that modifies other tables, GoArchive will not be aware of those side effects.
* To prevent data inconsistency, we recommend auditing your triggers before running a purge.

### 4. MySQL Versioning

Currently, GoArchive is optimized for **MySQL 8.0+** using the **InnoDB** storage engine.

* Legacy engines like MyISAM are strictly not supported due to the lack of transactional integrity required for the Copy-Verify-Delete cycle.

### 5. Trust Model for SQL Configuration

GoArchive intentionally treats configuration files as **operator-controlled and trusted** input.

* Job `where` values are raw SQL fragments injected into archive selection queries.
* Connections intentionally use `multiStatements=true` for operational compatibility.
* Do not expose config editing to untrusted users or untrusted automation pipelines.

### 6. Single-Column Primary Keys Only

GoArchive identifies, copies, verifies, and **deletes** rows by a single primary-key column (`WHERE pk IN (...)`).

* **Composite (multi-column) primary keys are not supported.** Any participating table whose `PRIMARY KEY` spans more than one column is rejected by preflight (`COMPOSITE_PK_CHECK`). A composite PK would cause the single-column filter to over-match and could delete rows that were never part of the archived set.
* **Root tables must additionally use an integer single-column PK** (TINYINT–BIGINT, signed or unsigned). Child tables may use any single-column PK type.
* If your schema uses composite keys, GoArchive Community edition cannot safely archive those tables.

---

> [!WARNING]
>  This tool performs data deletion on your source database. It is in use in limited production environments, but has not yet undergone exhaustive large-scale testing. **Rigorously test every archive job in a staging or test environment with representative data before running it against production.**


## ⚠️ Important Disclaimer

This tool can perform data deletion operations on your source database (`archive`, `purge`). 
- **Testing**: Always test your archive jobs on a staging system with a representative data set first.
- **Backups**: Ensure you have valid backups of your data before running archive or purge operations.
- **Verification**: Use the `dry-run` and `validate` commands to preview and verify your configuration before execution.

## Features

- **Automatic Dependency Resolution** - Uses Kahn's algorithm for topological sorting of related tables
- **Zero-Lock Processing** - Batch operations with configurable delays to minimize production impact
- **Crash Recovery** - Checkpoint-based resume capability with sub-minute recovery time
- **Data Integrity** - Optional count and SHA256 verification between source and destination
- **Replication Lag Monitoring** - Automatic pausing when replica lag exceeds thresholds
- **Dry-Run Mode** - Preview operations without making changes
- **Graceful Shutdown** - SIGTERM/SIGINT handling for clean interruption

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/dbsmedya/goarchive.git
cd goarchive

# Build the binary
go build -o goarchive ./cmd/goarchive

# Move to your PATH (optional)
sudo mv goarchive /usr/local/bin/
```

### Configuration

Create a configuration file `archiver.yaml`:

```yaml
# Source database (production - data to archive)
source:
  host: localhost
  port: 3306
  user: archiver
  password: change_me
  database: production
  max_connections: 10

# Destination database (archive storage)
destination:
  host: archive.db.internal
  port: 3306
  user: archiver
  password: change_me
  database: archive
  max_connections: 10

# Archive jobs configuration
jobs:
  archive_old_orders:
    root_table: orders
    primary_key: id
    where: "created_at < DATE_SUB(NOW(), INTERVAL 2 YEAR)"
    relations:
      - table: order_items
        primary_key: id
        foreign_key: order_id
        dependency_type: "1-N"

      - table: order_payments
        primary_key: id
        foreign_key: order_id
        dependency_type: "1-N"

# Processing settings
processing:
  batch_size: 1000
  batch_delete_size: 500
  sleep_seconds: 1

# Safety settings
safety:
  lag_threshold: 10
  check_interval: 5
```

See [configs/archiver.yaml.example](configs/archiver.yaml.example) for a complete example.

### Basic Usage

```bash
# Check the plan from yaml:
goarchive plan -c archiver.yaml --job archive_old_orders


=================
  Relation Tree
=================

┌────────────────┐                                                 [ Tree Summary ]
│                │                                                 ----------------
│     orders     ├─────1-N──────┐                                  Root Table:     orders
│                │     │        │                                  Relations:      4 tables
└────────┬───────┘     └────────┼─────1-1───────────────┐          Max Depth:      2 levels
         │                      │                       │          Destination DB: archive
         │                      │                       │
        1-N                     │                       │          [ Processing ]
         │                      │                       │          --------------
         ▼                      ▼                       ▼          Batch Size:      1000
┌────────────────┐     ┌────────────────┐         ┌───────────┐    Batch Delete:    500
│                │     │                │         │           │    Sleep:           1.0s
│  order_items   │     │ order_payments │         │ shipments │
│                │     │                │         │           │    [ Verification ]
└────────────────┘     └────────────────┘         └─────┬─────┘    ----------------
                                                        │          Method:          count
                                                        │
                                                        │
                                                       1-N
                                                        │
┌────────────────┐                                      │
│                │                                      │
│ shipment_items │◄─────────────────────────────────────┘
│                │
└────────────────┘


# Validate configuration and run preflight checks
goarchive validate -c archiver.yaml

# Preview what would be archived (dry-run)
goarchive dry-run -c archiver.yaml --job archive_old_orders

# Execute archive (runs preflight, copies to destination, verifies, then deletes)
goarchive archive -c archiver.yaml --job archive_old_orders

# Copy-only (runs non-destructive preflight, copies to destination, never deletes source)
goarchive copy-only -c archiver.yaml --job archive_old_orders

# Copy-only force mode (shows confirmation prompt before bypassing duplicate preflight)
goarchive copy-only -c archiver.yaml --job archive_old_orders --force

# Purge only (runs source-side preflight, then deletes without copying - USE WITH CAUTION!)
goarchive purge -c archiver.yaml --job archive_old_orders
```

## Commands

| Command | Description |
|---------|-------------|
| `archive` | Full archive workflow: discover → copy → verify → delete |
| `copy-only` | Copy + verify workflow without source deletion (prompts only with `--force`) |
| `purge` | Delete-only mode for data cleanup without archiving |
| `dry-run` | Preview execution plan with row count estimates |
| `validate` | Run configuration validation and preflight checks |
| `plan` | Display table dependency graph and processing order |
| `list-jobs` | List all configured archive jobs |
| `version` | Show version information |

### Global Flags

```
  -c, --config string       Path to configuration file (default "archiver.yaml")
      --log-level string    Override log level (debug, info, warn, error)
      --log-format string   Override log format (json, text)
      --skip-verify         Skip data verification after copy
```

> [!NOTE]
> Processing settings (`batch_size`, `batch_delete_size`, `sleep_seconds`, etc.) are
> config-file-only — there are no CLI flag overrides. Set them in the global
> `processing:` block or per-job `processing:` override block.

### Recommended operator workflow

1. `goarchive validate -c archiver.yaml` — config + full preflight for every job
2. `goarchive dry-run -c archiver.yaml -j <job>` — runs non-destructive preflight,
   shows the WHERE clause and the filtered row counts the run would actually
   touch, and validates payload limits against the destination (rolled back)
3. `goarchive archive -c archiver.yaml -j <job>`

`where` is required on every job; use `where: "1=1"` to deliberately process a
whole table. Destination tables may drop secondary indexes for write speed, but
source/destination **character sets must match** unless sha256 verification is
enabled **and not skipped** (`verification.method: sha256` with
`skip_verification: false`).

### Required privileges

| Server | Privileges | Used for |
|--------|-----------|----------|
| Source | `SELECT`, `DELETE` | reading and deleting archived rows |
| Destination (data tables) | `SELECT`, `INSERT` | copying rows into archive tables |
| Tracking schema (`job_schema`) | `CREATE`, `SELECT`, `INSERT`, `UPDATE` | creating and maintaining `archiver_job` and per-job `archiver_job_log_<id>` tables; `CREATE` is required at runtime because per-job log tables are created on the fly; `DELETE`/`DROP` are optional for DBA cleanup (`DROP` additionally needed for `TRUNCATE`) |
| Replica (optional) | `REPLICATION CLIENT` | lag monitoring (`SHOW REPLICA STATUS`) |

Preflight verifies destination `INSERT` and source `DELETE` up front (the two
that would otherwise fail mid-run, after copy has committed); the new
`JOB_SCHEMA_PERMISSION_CHECK` verifies `CREATE/SELECT/INSERT/UPDATE` on the
tracking schema at startup. The privilege checks themselves need no extra grants
— MySQL always exposes the current account's own privileges in
`information_schema`.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│   CLI       │────▶│   Config    │────▶│   Database Manager  │
│  (Cobra)    │     │  (Viper)    │     │   (Connection Pool) │
└─────────────┘     └─────────────┘     └─────────────────────┘
                                                │
                         ┌──────────────────────┼──────────────────────┐
                         ▼                      ▼                      ▼
                   ┌──────────┐          ┌──────────┐          ┌──────────────┐
                   │  Source  │          │  Archive │          │   Replica    │
                   │  (MySQL) │          │  (MySQL) │          │   (MySQL)    │
                   └──────────┘          └──────────┘          └──────────────┘
                         │                      │
                         └──────────────────────┘
                                    │
                         ┌──────────┴──────────┐
                         ▼                     ▼
                ┌─────────────────┐    ┌──────────────┐
                │  Graph Builder  │    │  Lag Monitor │
                │ (Kahn's Algo)   │    │              │
                └─────────────────┘    └──────────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
   ┌──────────┐   ┌──────────┐   ┌──────────┐
   │  Copy    │   │ Verify   │   │  Delete  │
   │  Phase   │──▶│ (Count/  │──▶│  Phase   │
   │          │   │ SHA256)  │   │          │
   └──────────┘   └──────────┘   └──────────┘
```

### Processing Pipeline

1. **Preflight Checks** - Validate configuration, check triggers, verify InnoDB
2. **Graph Build** - Parse table relations → Kahn's algorithm → copy order (parent-first), delete order (child-first)
3. **Batch Loop** - Fetch root IDs → BFS discovery → copy transaction → verify → delete
4. **Safety** - Advisory locks + destination job-state checks prevent concurrent archive/purge/copy-only overlap on the same root table; replication lag monitoring pauses processing

### Key Components

| Package | Purpose |
|---------|---------|
| `cmd/` | CLI command implementations (Cobra) |
| `internal/config/` | Configuration parsing with Viper |
| `internal/database/` | Database connection pooling and management |
| `internal/graph/` | Dependency graph builder with Kahn's algorithm |
| `internal/archiver/` | Core archive/purge/copy/delete logic |
| `internal/verifier/` | Count and SHA256 verification |
| `internal/lock/` | MySQL advisory lock implementation |
| `internal/logger/` | Structured logging with Zap |

## How It Works

### Dependency Resolution

GoArchive automatically determines the correct order for copying and deleting related records:

```
orders (root)
  ├── order_items (child)
  ├── order_payments (child)
  └── shipments (child)
        └── shipment_items (grandchild)

Copy Order:    orders → order_items → order_payments → shipments → shipment_items
Delete Order:  shipment_items → shipments → order_items → order_payments → orders
```

### Batch Processing

1. **Discovery** - BFS traversal finds all child records for a batch of root IDs
2. **Copy** - Transactional insert to destination in dependency order
3. **Verify** - Optional count/SHA256 verification ensures data integrity
4. **Delete** - Removes data from source in reverse dependency order
5. **Checkpoint** - Progress saved for crash recovery

### Tuning Throughput: batch_size and batch_delete_size

GoArchive processes root rows in batches. `batch_size` is the universal copy
chunk size: GoArchive fetches `batch_size` root PKs per batch, discovers their
full subgraph, and copies (fetch + insert) **every** table — root and children
— `batch_size` rows at a time. `batch_delete_size` is an independent throttle
controlling how many rows are deleted per statement (lower it to reduce
replication lag on the destination replica).

There are two independent pacing knobs, addressing two different pressures:

- **`sleep_seconds`** pauses **between batches** (after each `batch_size` batch).
  Use it to keep general load on the source/archive servers controllable.
- **`delete_sleep_seconds`** pauses **between delete chunks** (after each
  `batch_delete_size` delete, except the last chunk of each table). Use it to
  limit how fast the delete phase generates binary-log events, so a replica does
  not fall behind. Defaults to `0` (no delete throttle). Pair a small
  `batch_delete_size` with `delete_sleep_seconds` when replication lag — not
  source load — is your bottleneck. The pause applies between chunks *within* a
  table (the high-frequency case). Per-job `processing:` blocks use pointer
  semantics: any explicitly set field — including `0` — overrides the global;
  unset fields inherit.

**Always validate batch_size before a real run — follow this three-step flow:**

1. **`goarchive validate -c archiver.yaml`** — validates configuration syntax,
   database connectivity, InnoDB engine, FK indexes, trigger warnings, and other
   preflight checks for all configured jobs.

2. **`goarchive dry-run -c archiver.yaml --job archive_old_orders`** — runs the
   non-destructive preflight profile, prints the job's WHERE clause, estimates
   row counts filtered through the actual relation chain (not full-table counts),
   and validates that `batch_size` fits the destination's limits:
   - **Placeholder check (exact):** `batch_size × column_count` must be less than
     65,535 (MySQL's prepared-statement placeholder limit). This check runs even
     for empty tables — a wide table is caught before you have data.
   - **`max_allowed_packet` check (measured):** the dry-run copies a
     `batch_size`-sized sample into a destination transaction and immediately
     rolls it back — nothing is persisted. It fails fast and tells you to lower
     `batch_size` if a table's row width exceeds the packet limit.

   > **Note:** The packet check for child tables is approximate — child rows are
   > sampled arbitrarily rather than via full discovery (which would be too
   > expensive for a dry-run). The placeholder check is exact for every table.

3. **`goarchive archive -c archiver.yaml --job archive_old_orders`** — the real
   run: discover → copy → verify → delete, with crash recovery and replication
   lag monitoring.

If you skip dry-run and `batch_size` is too large, the real run fails fast on
the first copy chunk. Already-processed root PKs are checkpointed; the
interrupted batch's PKs are left in a resumable state and replayed automatically
on the next run after you lower `batch_size` in your config.

### Pausing a run: `sentinel_file`

`sentinel_file` is an operator pause switch. Set it to a full file path in the
`processing` block:

```yaml
processing:
  sentinel_file: /var/run/goarchive/pause.flag
```

Before each batch, GoArchive checks whether that file exists. **While the file is
present, processing pauses** and re-checks once per second; **remove the file to
resume.** Create it with `touch /var/run/goarchive/pause.flag` to pause an
in-flight run (e.g. to relieve a struggling replica) without killing the process,
and `rm` it to continue. Presence is the only signal — the file's contents are
ignored.

Notes:
- The pause is honored by `archive`, `purge`, and `copy-only`, at the start of
  every batch (including resume/recovery batches).
- The wait is interruptible: `Ctrl-C` / shutdown aborts a paused run immediately,
  leaving the current batch unprocessed and recoverable on the next run.
- A very long pause leaves database connections idle; keep MySQL `wait_timeout`
  comfortably above your expected pause duration so the job's advisory lock and
  pooled connections are not dropped.
- Empty (default) disables the switch.

### Crash Recovery

If interrupted, GoArchive can resume from the last checkpoint:

```sql
-- Check job status (tracking tables live in job_schema, default = destination database)
SELECT id, job_name, job_status, last_processed_root_pk_id
FROM archiver_job WHERE job_name = 'archive_old_orders';

-- Resume with same command
goarchive archive -c archiver.yaml --job archive_old_orders
```

## Requirements

- **Go**: 1.21 or later
- **MySQL**: 8.0+ with InnoDB storage engine
- **Network**: Access to source, destination, and optionally replica databases

## Database Permissions

See the [Required privileges](#required-privileges) table in the Commands section for
the full privilege matrix. In summary:

```sql
-- On source database
GRANT SELECT, DELETE ON production.* TO 'archiver'@'%';

-- On archive/destination database (data tables)
GRANT SELECT, INSERT ON archive.* TO 'archiver'@'%';

-- On the tracking schema (job_schema; defaults to the destination database).
-- DBA must CREATE DATABASE if using an isolated schema:
--   CREATE DATABASE goarchive;
-- Then grant:
GRANT CREATE, SELECT, INSERT, UPDATE ON goarchive.* TO 'archiver'@'%';
-- DELETE/DROP are optional for DBA cleanup; DROP is also needed for TRUNCATE.
-- If job_schema is the same as destination.database, a single combined grant works:
--   GRANT SELECT, INSERT, CREATE, UPDATE ON archive.* TO 'archiver'@'%';

-- On replica (optional, for replication lag monitoring)
GRANT REPLICATION CLIENT ON *.* TO 'archiver'@'%';
```

Preflight checks source `DELETE` and destination `INSERT` up front; a new
`JOB_SCHEMA_PERMISSION_CHECK` verifies `CREATE/SELECT/INSERT/UPDATE` on the
tracking schema. The privilege introspection itself requires no extra grants —
GoArchive reads the connected account's own rows in `information_schema`.

## Testing

GoArchive includes comprehensive tests including unit tests and integration tests.

### Quick Start

```bash
# Run only unit tests (fast, no database required)
go test -short ./...

# Run all tests (requires database setup)
make test-integration
```

### Integration Tests

Integration tests require two MySQL databases (source and destination). You can use Docker for local testing or point to existing test databases.

#### Prerequisites

**Option 1: Using Docker (Recommended for local development)**

```bash
# Start test databases
make test-up

# Verify databases are running
make test-status
```

**Option 2: Using existing databases**

Configure the integration test to use your existing MySQL instances (see Configuration below).

#### Configuration

Integration tests require database credentials. You have three options:

**Option A: Environment Variable (Quickest)**
```bash
export MYSQL_ROOT_PASSWORD=your_password
INTEGRATION_FORCE=true go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...
```

**Option B: Using Makefile**
```bash
export MYSQL_ROOT_PASSWORD=your_password
make test-integration
```

**Option C: Custom Config File**
```bash
# Create your own config file
cp internal/archiver/integration_test.yaml /path/to/my-config.yaml
# Edit with your credentials
export INTEGRATION_CONFIG=/path/to/my-config.yaml
INTEGRATION_FORCE=true go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...
```

#### Configuration File Format

Create `internal/archiver/integration_test.yaml`:

```yaml
databases:
  - name: source
    host: 127.0.0.1
    port: 3305
    user: root
    password: your_password_here  # Required!
    database: goarchive_test

  - name: destination
    host: 127.0.0.1
    port: 3307
    user: root
    password: your_password_here  # Required!
    database: goarchive_test

force: false  # Set to true to drop/recreate databases
fixture_path: testdata/customer_orders.sql
```

#### Running Tests

```bash
# Run all integration tests
make test-integration

# Run specific integration test
MYSQL_ROOT_PASSWORD=your_password go test -v \
  -run TestOrchestrator_FullArchiveCycle_Integration \
  ./internal/archiver/...

# Force database recreation (clean slate)
INTEGRATION_FORCE=true MYSQL_ROOT_PASSWORD=your_password \
  go test -v -run 'TestOrchestrator_.*_Integration' ./internal/archiver/...

# Stop test databases when done
make test-down
```

#### Available Integration Tests

| Test | Description |
|------|-------------|
| `TestOrchestrator_FullArchiveCycle_Integration` | End-to-end archive workflow |
| `TestOrchestrator_CrashRecovery_Integration` | Resume after simulated crash |
| `TestOrchestrator_ReplicationLagPause_Integration` | Lag monitoring behavior |
| `TestOrchestrator_VerificationMismatch_Integration` | Data verification logic |
| `TestOrchestrator_ContextCancellation_Integration` | Graceful shutdown handling |
| `TestOrchestrator_EmptyResultSet_Integration` | Empty result handling |
| `TestOrchestrator_MultiLevelHierarchy_Integration` | 3-level deep relationships |

### End-to-End (Sakila) Tests

E2E tests archive the Sakila sample database through the real CLI.

```bash
make e2e-setup   # bootstrap docker + DBs, then run the working Sakila E2E suite
```

📖 **[tests/README.md](tests/README.md) is the source of truth for all E2E and
integration testing** — environment setup, the full test matrix, the Sakila E2E
suite (working archives + validation-failure demos and their expected error
categories), single-test targeting, troubleshooting, and how to add a test.
Refer to it rather than duplicating those details here.

## Configuration Reference

### Source/Destination Database

| Option | Description | Default |
|--------|-------------|---------|
| `host` | Database host | required |
| `port` | Database port | 3306 |
| `user` | Username | required |
| `password` | Password | required |
| `database` | Database name | required |
| `tls` | TLS mode (disable/preferred/required) | preferred |
| `max_connections` | Max open connections | 10 |
| `max_idle_connections` | Max idle connections | 5 |

#### Destination-only options

| Option | Description | Default |
|--------|-------------|---------|
| `job_schema` | Schema holding GoArchive's tracking tables (`archiver_job`, `archiver_job_log_<id>`). A DBA must pre-create this schema and grant `CREATE, SELECT, INSERT, UPDATE` on it. The tool does **not** create schemas automatically. | Same as `database` |

The tracking tables stored in `job_schema`:
- **`archiver_job`** — one row per configured job; `id` is an integer `PRIMARY KEY`; `job_name` is a `UNIQUE KEY`. Checkpoint and heartbeat data live here.
- **`archiver_job_log_<id>`** — one per-job table, named by the job's integer `id`. Tracks per-root-PK status as a `TINYINT` (0=pending, 1=copied, 2=completed, 3=failed). No `job_name` column; no timestamps. Completed and failed rows are kept as evidence — they are not deleted automatically.

To look up which log table belongs to a job:
```sql
SELECT id, job_name FROM <job_schema>.archiver_job;
-- per-job log table: archiver_job_log_<id>
```

### Job Configuration

| Option | Description | Required |
|--------|-------------|----------|
| `root_table` | Primary table to archive | yes |
| `primary_key` | Primary key column | yes (default: `id`) |
| `where` | Raw SQL WHERE clause for filtering rows (trusted operator input) | yes |
| `relations` | Related tables to include | no |

### Processing Settings

| Option | Description | Default |
|--------|-------------|---------|
| `batch_size` | Root IDs processed per batch (universal copy chunk size) | 1000 |
| `batch_delete_size` | Rows per DELETE statement | 500 |
| `sleep_seconds` | Pause between batches (source/archive load throttle) | 1 |
| `delete_sleep_seconds` | Pause between delete chunks (replication/binlog throttle) | 0 |
| `sentinel_file` | Operator pause switch: while this file exists, pause before each batch (re-check every 1s) | _(empty)_ |

### Safety Settings

| Option | Description | Default |
|--------|-------------|---------|
| `lag_threshold` | Max replication lag in seconds | 10 |
| `check_interval` | Lag check frequency in seconds | 5 |
| `disable_foreign_key_checks` | Disable FK checks during copy | false |

## Upgrade Notes

### Resume metadata and advisory locks unified on destination

Previously `purge` wrote its `archiver_job` and `archiver_job_log` state tables
to the **source** database, while `archive` and `copy-only` wrote them to the
**destination** database. Advisory locks were similarly split (archive and
purge on source, copy-only on destination). This left gaps in the
cross-command concurrency check and made state hard to reason about.

After this upgrade:

- `archive`, `purge`, and `copy-only` all use the **destination** database for
  both `archiver_job`/`archiver_job_log` metadata and for advisory locks.
- `purge` now requires a destination connection to be configured (it was
  already implicitly required by validation, but is now load-bearing at
  runtime).

**DBA action required:** drop the now-unused state tables from the source
database after upgrading. Any checkpoint rows there are orphaned and will not
be migrated automatically:

```sql
-- On the SOURCE server only
DROP TABLE IF EXISTS archiver_job_log;
DROP TABLE IF EXISTS archiver_job;
```

No action is needed on the destination — archive and copy-only already wrote
there, and purge's existing job rows remain on source (harmless once dropped).

### Tracking table reshape (per-job log tables, integer id PK)

This release replaces the single shared `archiver_job_log` table with per-job
`archiver_job_log_<id>` tables and promotes `archiver_job.id` from a simple
column to the integer `PRIMARY KEY` (with `job_name` now a `UNIQUE KEY`).

**Not state-compatible with prior versions.** A startup probe detects old-shape
tables and exits with upgrade guidance. No auto-migration is performed.

**Before upgrading:**

1. Drain all in-flight jobs to completion.
2. Have a DBA drop the old tracking tables on the destination (or on your
   `job_schema` if you have configured a separate schema):

   ```sql
   DROP TABLE IF EXISTS archiver_job_log;
   DROP TABLE IF EXISTS archiver_job;
   ```

3. The new tables are created automatically on the next run.
4. If using an isolated `job_schema`, a DBA must `CREATE DATABASE <schema>` and
   grant `CREATE, SELECT, INSERT, UPDATE` on it before running the upgraded binary.

Per-job log tables (`archiver_job_log_<id>`) are not deleted automatically —
completed and failed rows are kept as evidence. A DBA may `DROP` or `TRUNCATE`
them for housekeeping as needed.

### FOREIGN_KEY_CHECKS handling hardened

`safety.disable_foreign_key_checks` now runs on a dedicated destination
connection and is explicitly reset before the connection returns to the pool.
Previously, enabling it could leak `FOREIGN_KEY_CHECKS=0` into other pooled
destination operations.

The option remains **disabled by default**. If you enable it, `goarchive
validate` and every copy run will emit a loud warning — use only when you have
verified the copy order and accept the risk of inserting rows that bypass FK
constraints.

## Project Status

- **Edition**: Community
- **Version**: `1.3.2-community` (stable for the scope below)
- **Recommended for**: single-operator workstation archival of cold MySQL data
- **Test coverage**: extensive unit tests (sqlmock, no DB), real-MySQL integration tests (`-tags=integration`), and a focused Sakila E2E suite (working archives + preflight-validation demos) — see [tests/README.md](tests/README.md)

### Known Limits & Caution ⚠️

The community edition is suitable for the scope described above. Operators
should be aware of the following known limits before pointing it at real data:

- **Deep or wide graphs can exhaust memory.** BFS discovery accumulates all
  descendant primary keys per root batch in memory. Deeply nested schemas
  (parent → child → grandchild → great-grandchild, each 1-N with high fanout)
  can grow the accumulator unbounded. If your root table has ~1M matching rows
  and each has many descendants per level, start with a small `batch_size`
  (e.g. 100) and scale up only after observing memory.
- **Copy-phase transaction spans all tables.** One destination transaction
  covers the entire copy phase, which holds row locks on already-inserted
  tables while later tables are still streaming. Avoid running this against a
  shared destination that other workloads are reading.
- **No built-in metrics or telemetry.** Operators monitor progress through
  the structured log output and by querying the per-job `archiver_job_log_<id>` table directly (look up `id` from `archiver_job` by `job_name`).
- **Sequential by design.** One root PK at a time, one job at a time per
  destination. Advisory locks plus heartbeat-aware same-root checks prevent
  concurrent runs of the same job name or root table.
- **Advisory lock sessions must stay alive.** GoArchive keeps the job
  `GET_LOCK()` connection alive and aborts if ownership is lost. MySQL
  `wait_timeout` should be higher than the longest expected job duration; very
  low timeout or flaky network settings can correctly fail a job instead of
  letting it delete without a lock.
- **Primary keys must be single-column; root PKs must also be integer.**
  Composite (multi-column) primary keys on any participating table are rejected
  by preflight (`COMPOSITE_PK_CHECK`) because rows are identified and deleted by
  one PK column — a composite PK would over-match and risk deleting rows outside
  the archived set. Root tables additionally require an integer single-column PK
  (TINYINT through BIGINT, signed or unsigned); UUID, VARCHAR, DECIMAL, FLOAT,
  datetime, and other non-integer root PKs are rejected. Child tables may use any
  single-column PK type.
- **Runtime preflight is automatic.** `archive`, `purge`, and `copy-only` run
  preflight at startup before any `archiver_job` state is written. `validate`
  remains useful for inspecting issues before an operational run. Use
  `--skip-validate-preflight` only for documented recovery scenarios after
  manually verifying schema safety.
- **Trigger override is explicit.** For schemas with DELETE triggers (e.g.
  Sakila's `del_film`), `archive` and `purge` require `--force-triggers` after
  you've reviewed what those triggers do. `copy-only` skips DELETE-trigger
  checks because it never deletes from source.
- **`--force` is best-effort takeover, not hard exclusion.** It proceeds past
  advisory lock contention only when the previous holder's heartbeat is stale,
  and then refreshes the heartbeat so additional startups are blocked. A stale
  heartbeat does not prove the old process is dead; it may still own MySQL's
  `GET_LOCK()` and continue deleting. Operators must verify the old process is
  actually dead before forcing. It cannot bypass a live heartbeating job, the
  same-root concurrency check, or preflight.
- **Partial auto-commit deletes are expected after interruption.** Deletes are
  intentionally committed in batches to avoid long source locks. If a run stops
  between child and parent deletes, the source can temporarily have children
  removed while the parent remains. This is not data loss because rows were
  copied and verified first; resume completes the remaining work.
- **Shared/M-N child rows are outside the automatic model.** GoArchive deletes
  discovered child rows with the first referencing root. Membership/shared rows
  inside a many-to-many-style subgraph can be deleted earlier than another root
  expects; model those relationships explicitly and validate on staging.
- **Verification method controls dirty-destination behavior.** In
  `verification.method: count`, archive uses plain `INSERT` and aborts on any
  pre-existing destination row with the same key before deleting source data.
  In `verification.method: sha256`, archive uses `INSERT IGNORE` and verifies
  destination content by hash, which is the recommended recovery mode for
  interrupted jobs with pending PKs.
- **Schema-stable assumption.** GoArchive assumes source and destination
  schemas do not change during a batch loop. Run schema migrations either
  before or after archive jobs, never concurrently.
- **Data loss on misconfiguration is possible.** A misconfigured job that
  passes validate but has the wrong `WHERE` clause can delete the wrong rows
  from source. Always run `goarchive dry-run` and review the estimated row
  counts before executing `archive`.
- **Upgrade caveat: not state-compatible with prior versions.** This release
  reshapes tracking tables (`archiver_job` now has an integer `id` PRIMARY KEY;
  the shared `archiver_job_log` table is replaced by per-job
  `archiver_job_log_<id>` tables). A startup probe detects old-shape tables and
  rejects them with upgrade guidance — no auto-migration is performed.
  **Before upgrading:** drain all in-flight jobs to completion, then have a DBA
  drop the old tables on the destination (or on `job_schema` if isolated):
  ```sql
  DROP TABLE IF EXISTS archiver_job_log;
  DROP TABLE IF EXISTS archiver_job;
  ```
  They are recreated automatically on the next run. If you use an isolated
  `job_schema`, a DBA must pre-create that schema and grant
  `CREATE, SELECT, INSERT, UPDATE` before running the upgraded binary. Per-job
  log tables (`archiver_job_log_<id>`) accumulate over time as jobs are created;
  a DBA may `DROP` or `TRUNCATE` them for housekeeping (evidence rows are not
  deleted automatically).

### What's Included in Community

Complete end-to-end archive, purge, and copy-only workflows:
- Dependency graph + topological copy / reverse-topological delete order
- Preflight checks: storage engine, FK indexes, FK coverage (external + internal),
  destination schema compatibility, destination write permissions, DELETE
  triggers, INSERT triggers on destination, CASCADE warnings
- Crash recovery via `archiver_job` + per-job `archiver_job_log_<id>` tables in `job_schema` (destination by default)
- Advisory locks serialize job-name execution across all three commands
- Replication lag monitor (pauses batches when replica lag exceeds threshold)
- Verification by row count or SHA256
- Dry-run mode with execution plan output

### Planned for Enterprise

- Archive to BigQuery
- Observability: Prometheus metrics, OpenTelemetry traces, dashboards
- Parallelism: multi-root-PK concurrent processing, pipelining copy/verify/delete
- Admin API for runtime pause / resume / inspect
- Multi-tenancy and horizontal scale
- Adaptive rate limiting
- Web based GUI

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Cobra](https://github.com/spf13/cobra) - CLI framework
- [Viper](https://github.com/spf13/viper) - Configuration management
- [Zap](https://github.com/uber-go/zap) - Structured logging
- [MySQL Driver](https://github.com/go-sql-driver/mysql) - Go MySQL driver
- [mermaid-ascii](https://github.com/AlexanderGrooff/mermaid-ascii) - ASCII diagram generation for table relationship visualization
