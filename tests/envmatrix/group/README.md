# Group Replication source for the environment matrix

`compose_group_replication.yaml` and `gr-bootstrap.sh` are copied unchanged from dbsgomysql
**v1.4.0** (`tests/docker/`), the library's own Group Replication test topology: a
three-member single-primary InnoDB Cluster (gr1 the primary) and three standalone servers.
Copy them again, unchanged, when the dbsgomysql pin moves and the library's files change.

`run-matrix.sh` uses them for rows of kind `group` (`rows.tsv`): it starts only `gr1`, `gr2`,
`gr3` and `gr-bootstrap` (with `gr-ext`, the bootstrap's dependency), gr1 is the source, a plain
server of the same image is the destination, and only the unfinished-work guard tests run,
because Group Replication refuses tables without a primary key, which other suites create on
purpose. The cluster's root password is `root`.
