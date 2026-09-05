# GoArchive Documentation

Detailed specifications for GoArchive. Start at the
[project README](../README.md) for the overview, basic usage, and architecture.

| Document | Covers |
|----------|--------|
| [Configuration](README_CONFIGURATION.md) | Every config block, option, default, and precedence rule; identifier rules; tracking tables |
| [Validation & Preflight](README_VALIDATION.md) | All 20 named checks (19 preflight, plus the connection-time identity check), the check-to-command matrix, schema compatibility rules, dry-run payload validation |
| [Permissions](README_PERMISSIONS.md) | Privilege matrix, grant recipes, the invariants preflight enforces, troubleshooting |
| [Limitations](README_LIMITATIONS.md) | Hard constraints, model limitations, operational cautions, trust model |
| [Operations](README_OPERATIONS.md) | Commands and flags, operator workflow, tuning, pausing, crash recovery and resume semantics |
| [Job Tracking Schema](README_JOBS_SCHEMA.md) | DBA guide: tracking table structures, inspection queries, what is safe to truncate |
| [Testing](README_TESTING.md) | Test layers and how to run them |
| [Upgrading to 2.2](README_UPGRADING_2_2.md) | UTC sessions, the tracking-schema 2.2 refusal and its remedy, what `where` means now |
| [Upgrading to 2.1](README_UPGRADING_2_1.md) | Migrating the removed `replica:` block and `safety:` lag keys to `replication:` |
| [Upgrading to 2.0](README_UPGRADING_2_0.md) | What changes when moving from 1.8, and what to do about it |
| [Why GoArchive uses dbsgomysql](README_dbsgomysql.md) | What the validation library provides, the fact/policy split, and the consumer boundary |

## Elsewhere in the repository

| Location | Covers |
|----------|--------|
| [`../README.md`](../README.md) | Overview, philosophy, basic usage, architecture |
| [`../INSTALL.md`](../INSTALL.md) | Installation and build reference |
| [`../tests/README.md`](../tests/README.md) | **Source of truth** for the full integration and E2E test matrix |
| [`../configs/archiver.yaml.example`](../configs/archiver.yaml.example) | Annotated example configuration |

## Where to start

- **Setting up a first job** → [Configuration](README_CONFIGURATION.md), then
  [Permissions](README_PERMISSIONS.md)
- **A preflight check is failing** → [Validation & Preflight](README_VALIDATION.md)
- **Deciding whether GoArchive fits your schema** → [Limitations](README_LIMITATIONS.md)
- **A run is too slow, or needs pausing or resuming** → [Operations](README_OPERATIONS.md)
- **Upgrading to 2.2, and startup refuses your tracking tables** → [Upgrading to 2.2](README_UPGRADING_2_2.md)
- **Upgrading, and validation now rejects a key that used to work** → [Upgrading to 2.1](README_UPGRADING_2_1.md)
- **Maintaining the tracking tables, or clearing a crashed job** → [Job Tracking Schema](README_JOBS_SCHEMA.md)
