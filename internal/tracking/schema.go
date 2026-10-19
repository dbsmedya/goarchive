package tracking

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"

	"github.com/dbsmedya/goarchive/v3/internal/lock"
)

const (
	jobTableName  = "archiver_job"
	metaTableName = "goarchive_meta"
)

// bindingColumns are the 3.0 columns of archiver_job that no 2.x release has. A
// marker-less archiver_job without them is a 2.x table; under a 3.0 label, one without them
// is refused too (a pre-alpha 3.0 table lacks destination_server_uuid).
var bindingColumns = []string{"source_server_uuid", "source_database", "destination_database", "destination_server_uuid",
	"config_fingerprint", "root_key_columns"}

func (s *Store) createJobTableSQL() string {
	// job_name is ASCII (R37) and unique without regard to letter case, so two configs
	// cannot hold "Orders" and "orders" as two jobs in one job_schema.
	// destination_server_uuid is reserved for GH #14's destination binding: alpha.0 neither
	// writes nor compares it, and '' means not recorded.
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  root_table VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  job_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  job_status TINYINT NOT NULL DEFAULT 0,
  source_server_uuid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  source_database VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  destination_database VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  destination_server_uuid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  config_fingerprint TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  root_key_columns TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  last_heartbeat_at DATETIME NULL,
  UNIQUE KEY uk_job_name (job_name),
  KEY idx_status (job_status)
) ENGINE=InnoDB`, s.jobTable)
}

// createMetaTableSQL is the one-row label table, the same shape 2.x used, so a 2.x
// schema's label is read by the same SELECT and refused.
func (s *Store) createMetaTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  id TINYINT NOT NULL PRIMARY KEY,
  schema_version VARCHAR(16) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB`, s.metaTable)
}

// schemaState is what the job_schema holds before Open changes anything.
type schemaState struct {
	meta      bool   // goarchive_meta exists
	label     string // its id=1 schema_version
	labelled  bool   // the id=1 row exists
	job       bool   // archiver_job exists
	jobIs30   bool   // archiver_job has every 3.0 binding column
	jobHasRow bool   // archiver_job holds at least one row
}

// legacyLabels are the labels GoArchive 2.x wrote: 2.0 (1.9.8-RC to 2.1.x) and 2.2. The set is
// closed. Only these, and a 2.x-shaped archiver_job, get the 2.x drop procedure (SLR-1).
var legacyLabels = map[string]bool{"2.0": true, "2.2": true}

// remedyUnknownLabel is the procedure for any other label. The label is not ordered: it may be a
// stale schema an earlier release left, or a current one a later release wrote.
const remedyUnknownLabel = "This GoArchive did not write that label: an earlier or a later GoArchive release did. " +
	"Find out which before changing anything. Tables left by an earlier release are retired with that release: " +
	"finish its jobs with its binary, then drop them. Tables written by a later release belong to it: " +
	"run that release, and do not drop them. Either way, pointing destination.job_schema at another schema " +
	"leaves them untouched. See \"Tracking-schema upgrade procedures\" in docs/README_JOBS_SCHEMA.md"

// classify decides spec §6.1 and R37 for one state. Every label but 3.0 is refused, and so
// is an unlabelled schema unless it is new: 2.x wrote its label before any job row, and
// releases before 1.8 wrote none. A 2.x-shaped archiver_job is refused whatever its label.
// A 3.0 label without archiver_job is refused too: its job ids would restart at 1 and
// meet the logs of the jobs that held them.
func classify(schema string, st schemaState) error {
	switch {
	case st.labelled && st.label != SchemaLabel && legacyLabels[st.label]:
		return &SchemaError{Schema: schema, Found: fmt.Sprintf("report schema_version %q", st.label)}
	case st.labelled && st.label != SchemaLabel:
		// Before the 2.x-shape case: a later release may change archiver_job itself.
		return &SchemaError{Schema: schema, Found: fmt.Sprintf("report schema_version %q", st.label),
			Remedy: remedyUnknownLabel}
	case st.job && !st.jobIs30:
		// Checked before the label is trusted: relabelling a 2.x schema is the shortcut
		// R37's procedure exists to prevent.
		how := "without a schema_version label"
		if st.labelled {
			how = fmt.Sprintf("under schema_version %q", st.label)
		}
		return &SchemaError{Schema: schema, Found: "hold a 2.x archiver_job table " + how}
	case st.labelled && !st.job:
		return &SchemaError{Schema: schema, Found: "are labelled 3.0 but have no archiver_job table",
			Remedy: "The per-job logs archiver_job_log_<id> would meet new jobs that reuse their ids. Restore archiver_job " +
				"from a backup, or drop every tracking table (goarchive_meta and every archiver_job_log_<id>) to start afresh"}
	case st.labelled:
		return nil
	case st.job && st.jobHasRow:
		return &SchemaError{Schema: schema, Found: "hold job rows but no schema_version label",
			Remedy: "These are 3.0 tables whose label row is missing. After confirming GoArchive 3.0 wrote them, restore it with " +
				"INSERT INTO goarchive_meta (id, schema_version) VALUES (1, '3.0'), or drop every tracking table to start afresh"}
	default:
		return nil
	}
}

// readState reads the schema's state with the library's table facts and GoArchive's own
// SELECTs on its own tables. It never queries information_schema (CLAUDE.md).
func (s *Store) readState(ctx context.Context) (schemaState, error) {
	var st schemaState
	infos, err := s.facts.Tables(ctx, []string{jobTableName, metaTableName})
	if err != nil {
		return st, fmt.Errorf("tracking: list tracking tables in %q: %w", s.schema, err)
	}
	for _, info := range infos {
		switch info.Table {
		case jobTableName:
			st.job = true
		case metaTableName:
			st.meta = true
		}
	}
	if st.meta {
		err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT schema_version FROM %s WHERE id = 1", s.metaTable)).Scan(&st.label)
		switch {
		case err == nil:
			st.labelled = true
		case !errors.Is(err, sql.ErrNoRows):
			return st, fmt.Errorf("tracking: read the tracking-schema label in %q: %w", s.schema, err)
		}
	}
	if st.job {
		spec, err := s.facts.TableSpec(ctx, jobTableName)
		if err != nil {
			return st, fmt.Errorf("tracking: read %s.%s: %w", s.schema, jobTableName, err)
		}
		st.jobIs30 = hasColumns(spec, bindingColumns)
	}
	if st.job && !st.labelled {
		var one int
		err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", s.jobTable)).Scan(&one)
		switch {
		case err == nil:
			st.jobHasRow = true
		case !errors.Is(err, sql.ErrNoRows):
			return st, fmt.Errorf("tracking: read %s.%s: %w", s.schema, jobTableName, err)
		}
	}
	return st, nil
}

func hasColumns(spec validations.TableSpec, names []string) bool {
	for _, n := range names {
		if !slices.ContainsFunc(spec.Columns, func(c validations.ColumnSpec) bool { return c.Name == n }) {
			return false
		}
	}
	return true
}

// initLockWait is how long a run waits for another run creating the same new tracking
// schema (TRK-1). Creating it takes well under a second; a test shortens this.
var initLockWait = 60 * time.Second

// SchemaLockedError refuses a run: another session held the schema lock of a new tracking
// schema for the whole wait (TRK-1). The run did nothing.
type SchemaLockedError struct {
	Schema string
	Wait   time.Duration
	Lock   *lock.HeldError
}

func (e *SchemaLockedError) Error() string {
	head := fmt.Sprintf("tracking: another run held the schema lock %s on tracking schema %q for the whole %ds wait",
		e.Lock.Name, e.Schema, int(e.Wait/time.Second))
	if e.Lock.Holder == 0 {
		return head + " and released it a moment ago. Nothing was done. Run again"
	}
	return fmt.Sprintf("%s; connection %d still holds it. Nothing was done. Run again when it finishes, "+
		"or check that connection (SHOW PROCESSLIST)", head, e.Lock.Holder)
}

func (e *SchemaLockedError) Unwrap() error { return e.Lock }

// ensureSchema refuses a schema this binary does not recognize, before writing anything to
// it, and otherwise creates the tracking tables and stamps the label. A labelled schema is
// judged at once, with no lock and no DDL: the label is written last and no 3.0 run changes
// it. Anything else is judged and created under the schema lock, one run at a time (TRK-1):
// a run reading while another is mid-way could see half of its work.
func (s *Store) ensureSchema(ctx context.Context) error {
	st, err := s.readState(ctx)
	if err != nil {
		return err
	}
	if st.labelled {
		return classify(s.schema, st)
	}
	start := time.Now()
	release, err := lock.Wait(ctx, s.db, lock.SchemaLockName(s.schema), initLockWait)
	if err != nil {
		var held *lock.HeldError
		if errors.As(err, &held) {
			return &SchemaLockedError{Schema: s.schema, Wait: initLockWait, Lock: held}
		}
		return fmt.Errorf("tracking: take the schema lock of %q: %w", s.schema, err)
	}
	defer release()
	s.initWait = time.Since(start)
	if st, err = s.readState(ctx); err != nil {
		return err
	}
	if err := classify(s.schema, st); err != nil {
		return err
	}
	if st.labelled {
		return nil // another run created it while this one waited
	}
	for _, ddl := range []string{s.createJobTableSQL(), s.createMetaTableSQL()} {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("tracking: create the tracking tables in schema %q (does it exist, and does the account "+
				"hold CREATE, SELECT, INSERT and UPDATE on it?): %w", s.schema, err)
		}
	}
	// The label is written last, so a labelled schema is complete. Under the schema lock no
	// 3.0 run stamps at the same time; ON DUPLICATE KEY keeps the first row if a run of an
	// earlier build, which takes no lock, stamps it too.
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, schema_version) VALUES (1, ?) ON DUPLICATE KEY UPDATE id = id", s.metaTable), SchemaLabel); err != nil {
		return fmt.Errorf("tracking: stamp tracking-schema label %s in %q: %w", SchemaLabel, s.schema, err)
	}
	var label string
	if err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT schema_version FROM %s WHERE id = 1", s.metaTable)).Scan(&label); err != nil {
		return fmt.Errorf("tracking: read back the tracking-schema label in %q: %w", s.schema, err)
	}
	if label != SchemaLabel {
		return &SchemaError{Schema: s.schema, Found: fmt.Sprintf("report schema_version %q", label)}
	}
	return nil
}
