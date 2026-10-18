# GoArchive Documentation

Detailed specifications for GoArchive. Start at the
[project README](../README.md) for the overview, basic usage, and architecture.

| Document | Covers |
|----------|--------|
| [Configuration](README_CONFIGURATION.md) | Every config block, option, default, and precedence rule; the `where` rules; identifier rules |
| [Validation & Preflight](README_VALIDATION.md) | Every preflight and connection-time check, the checks by job type, schema compatibility rules, SHA256 verification |
| [Permissions](README_PERMISSIONS.md) | Grant recipes, the privilege matrix, the provable-grant rule, troubleshooting |
| [Limitations](README_LIMITATIONS.md) | Hard constraints and the run contract, model limitations, operational cautions, trust model, supported versions |
| [Operations](README_OPERATIONS.md) | Commands and flags, operator workflow, tuning and memory, pausing, replication gating, crash recovery and resume, concurrency and locking |
| [Job Tracking Schema](README_JOBS_SCHEMA.md) | DBA guide: the tracking tables, the startup checks, inspection queries, what is safe to delete, failover re-binding |
| [Duplicate cleanup](README_DUPLICATE_CLEANUP.md) | Removing duplicate rows with one job: keep the minimum primary key per key |
| [Testing](README_TESTING.md) | What each test layer proves |
| [Upgrading to 3.0](README_UPGRADING_3_0.md) | Moving from 2.x and from 3.0 alpha.0: the tracking tables, configuration and command-line changes, changed limits |
| [Character sets](README_CHARSETS.md) | The session pin, the supported and refused character sets, converting a refused column |
| [MySQL compatibility notes](README_COMPAT.md) | MySQL server behaviours that change how GoArchive binds keys, the path it takes for each, and what is still open |
| [Why GoArchive uses dbsgomysql](README_dbsgomysql.md) | What the validation library provides, the fact/policy split, and the consumer boundary |

## Elsewhere in the repository

| Location | Covers |
|----------|--------|
| [`../README.md`](../README.md) | Overview, philosophy, basic usage, architecture |
| [`../INSTALL.md`](../INSTALL.md) | Installing (release binary, published image, from source) and building |
| [`../tests/README.md`](../tests/README.md) | **Source of truth** for running every test layer |
| [`../configs/archiver.yaml.example`](../configs/archiver.yaml.example) | Annotated example configuration |

## Where to start

- **Setting up a first job** → [Configuration](README_CONFIGURATION.md), then
  [Permissions](README_PERMISSIONS.md)
- **A preflight check is failing** → [Validation & Preflight](README_VALIDATION.md)
- **A column's character set is refused** → [Character sets](README_CHARSETS.md)
- **Deciding whether GoArchive fits your schema** → [Limitations](README_LIMITATIONS.md)
- **A run is too slow, or needs pausing or resuming** → [Operations](README_OPERATIONS.md)
- **Maintaining the tracking tables, or clearing a crashed job** → [Job Tracking Schema](README_JOBS_SCHEMA.md)
- **Upgrading from 2.x** → [Upgrading to 3.0](README_UPGRADING_3_0.md)
