package preflight

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// requiredStorageEngine is the only engine GoArchive supports: copy, verify and delete
// need transactions.
const requiredStorageEngine = "InnoDB"

// jobSchemaPrivileges are what the tracking schema needs. CREATE is needed at run time,
// because each job's log table is created on first use.
var jobSchemaPrivileges = []validations.Privilege{
	validations.PrivilegeCreate,
	validations.PrivilegeSelect,
	validations.PrivilegeInsert,
	validations.PrivilegeUpdate,
}

// checkTablesExist refuses a job table the source lacks, or one that is not a base table.
func checkTablesExist(ctx context.Context, f *facts) error {
	found, err := f.sourceTables(ctx)
	if err != nil {
		return inspectionError("table_existence", err)
	}
	return judgeTablesExist(f.tables, found, "TABLE_EXISTENCE_CHECK", "Tables not found in source database",
		"Only base tables can be archived. These source objects are not base tables; "+
			"remove them from the configuration, or archive the underlying tables instead")
}

// checkDestinationTablesExist is the same check on the destination.
func checkDestinationTablesExist(ctx context.Context, f *facts) error {
	found, err := f.destTables(ctx)
	if err != nil {
		return inspectionError("destination_table_existence", err)
	}
	return judgeTablesExist(f.tables, found, "DEST_TABLE_EXISTENCE_CHECK", "Tables not found in destination database",
		"Only base tables can receive archived rows. These destination objects are not base tables; "+
			"create real tables in the destination schema")
}

func judgeTablesExist(tables []string, found []validations.TableInfo, id, missing, nonBase string) error {
	findings := validations.CheckTablesExist(tables, found)
	for _, f := range findings {
		if f.Check != validations.IDTablesExist {
			return unexpectedFinding(strings.ToLower(id), f)
		}
	}
	if e := findingsError(id, missing, findings, validations.IDTablesExist); e != nil {
		return e
	}
	if objs := nonBaseTables(found); len(objs) > 0 {
		return &Error{Check: id, Message: nonBase, Tables: objs}
	}
	return nil
}

// checkStorageEngine refuses a table that is not InnoDB. A NULL engine is reported as
// <unknown> rather than passing.
func checkStorageEngine(ctx context.Context, f *facts) error {
	found, err := f.sourceTables(ctx)
	if err != nil {
		return inspectionError("storage_engine", err)
	}
	var offenders []string
	for _, fd := range validations.CheckStorageEngine(found, requiredStorageEngine) {
		if fd.Check != validations.IDStorageEngine {
			return unexpectedFinding("storage_engine", fd)
		}
		info, ok := fd.Facts.(validations.TableInfo)
		if !ok {
			return unexpectedFacts("storage_engine", fd, "validations.TableInfo")
		}
		engine := info.Engine
		if engine == "" {
			engine = "<unknown>"
		}
		offenders = append(offenders, info.Table+"("+engine+")")
	}
	if len(offenders) > 0 {
		return &Error{Check: "STORAGE_ENGINE_CHECK", Message: "Only InnoDB tables are supported. Use ALTER TABLE to convert", Tables: offenders}
	}
	return nil
}

// checkJobSchemaPermissions needs CREATE, SELECT, INSERT and UPDATE on the tracking schema,
// each provable for the account.
func checkJobSchemaPermissions(ctx context.Context, f *facts) error {
	p, err := f.destPrivileges(ctx)
	if err != nil {
		return inspectionError("job_schema", err)
	}
	findings := p.Schema(f.cfg.JobSchema, jobSchemaPrivileges)
	offenders, err := privilegeOffenders("job_schema", findings)
	if err != nil || len(offenders) == 0 {
		return err
	}
	missing := make([]string, 0, len(findings))
	needsCreate := false
	for _, fd := range findings {
		fact := fd.Facts.(validations.PrivilegeFact) // checked by privilegeOffenders
		missing = append(missing, fact.Privilege.String())
		needsCreate = needsCreate || fact.Privilege == validations.PrivilegeCreate
	}
	hint := fmt.Sprintf("GRANT %s ON `%s`.* TO <user>", strings.Join(missing, ", "), f.cfg.JobSchema)
	if needsCreate {
		hint = fmt.Sprintf("CREATE DATABASE `%s`; %s", f.cfg.JobSchema, hint)
	}
	return &Error{Check: "JOB_SCHEMA_PERMISSION_CHECK", Message: fmt.Sprintf(
		"destination account lacks provable %s on tracking schema %q (states: %s). Grant each missing "+
			"privilege directly to the account at schema scope (DBA must: %s)",
		strings.Join(missing, ", "), f.cfg.JobSchema, strings.Join(offenders, ", "), hint)}
}

// checkTablePrivilege needs privilege p, provably, on every job table of one server.
func checkTablePrivilege(stage, id, message string, p privileges, schema string, tables []string, priv validations.Privilege) error {
	offenders, err := privilegeOffenders(stage, p.Tables(schema, tables, priv))
	if err != nil {
		return err
	}
	if len(offenders) > 0 {
		return &Error{Check: id, Message: message, Tables: offenders}
	}
	return nil
}

func checkDestinationWritePermissions(ctx context.Context, f *facts) error {
	p, err := f.destPrivileges(ctx)
	if err != nil {
		return inspectionError("destination_write", err)
	}
	return checkTablePrivilege("destination_write", "DEST_WRITE_PERMISSION_CHECK",
		"Destination account lacks provable INSERT privilege on required tables. Grant it directly "+
			"to the account (GRANT INSERT ON <schema>.<table> TO <user>, or on <schema>.*)",
		p, f.cfg.DestinationDatabase, f.tables, validations.PrivilegeInsert)
}

func checkSourceSelectPermissions(ctx context.Context, f *facts) error {
	p, err := f.sourcePrivileges(ctx)
	if err != nil {
		return inspectionError("source_select", err)
	}
	return checkTablePrivilege("source_select", "SOURCE_SELECT_PERMISSION_CHECK",
		"Source account lacks provable SELECT privilege on required tables. Grant it directly to "+
			"the account (GRANT SELECT ON <schema>.* TO <user>)",
		p, f.cfg.SourceDatabase, f.tables, validations.PrivilegeSelect)
}

func checkSourceDeletePermissions(ctx context.Context, f *facts) error {
	p, err := f.sourcePrivileges(ctx)
	if err != nil {
		return inspectionError("source_delete", err)
	}
	return checkTablePrivilege("source_delete", "SOURCE_DELETE_PERMISSION_CHECK",
		"Source account lacks provable DELETE privilege on required tables. Grant it directly to "+
			"the account (GRANT DELETE ON <schema>.<table> TO <user>, or on <schema>.*)",
		p, f.cfg.SourceDatabase, f.tables, validations.PrivilegeDelete)
}

// checkDestinationInsertTriggers refuses destination INSERT triggers. --force-triggers
// does not apply: a trigger would change copied rows before verification reads them.
func checkDestinationInsertTriggers(ctx context.Context, f *facts) error {
	triggers, err := f.destInsertTriggers(ctx)
	if err != nil {
		return inspectionError("destination_insert_triggers", err)
	}
	tables, err := triggerOffenders("destination_insert_triggers", validations.CheckTriggersPresent(triggers, validations.TriggerInsert))
	if err != nil || len(tables) == 0 {
		return err
	}
	return &Error{Check: "DEST_INSERT_TRIGGER_CHECK",
		Message: "Destination INSERT triggers detected. Disable triggers before running archive copy operations", Tables: tables}
}

// checkDeleteTriggers refuses source DELETE triggers unless --force-triggers accepts them.
func checkDeleteTriggers(ctx context.Context, f *facts, force bool) error {
	triggers, err := f.sourceDeleteTriggers(ctx)
	if err != nil {
		return inspectionError("delete_triggers", err)
	}
	tables, err := triggerOffenders("delete_triggers", validations.CheckTriggersPresent(triggers, validations.TriggerDelete))
	if err != nil || len(tables) == 0 {
		return err
	}
	if force {
		f.cfg.Logger.Warnf("DELETE triggers detected (proceeding due to --force-triggers): %v", tables)
		return nil
	}
	return &Error{Check: "DELETE_TRIGGER_CHECK",
		Message: "DELETE triggers detected. Use --force-triggers to override (not recommended, triggers will fire during delete)", Tables: tables}
}

// warnCascadeRules warns about ON DELETE CASCADE. Cascade is legitimate design, so this
// never fails.
func warnCascadeRules(ctx context.Context, f *facts) error {
	in, err := f.foreignKeys(ctx, fkIncoming)
	if err != nil {
		return inspectionError("cascade", err)
	}
	out, err := f.foreignKeys(ctx, fkOutgoing)
	if err != nil {
		return inspectionError("cascade", err)
	}
	edges, err := cascadeEdges("cascade", validations.CheckCascadeRules(in.Keys), validations.CheckCascadeRules(out.Keys))
	if err != nil {
		return err
	}
	if len(edges) > 0 {
		f.cfg.Logger.Warnf("ON DELETE CASCADE rules detected (%d): %v", len(edges), edges)
		f.cfg.Logger.Warn("CASCADE rules may cause automatic deletion of related records. Verify this is intended behavior.")
	}
	return nil
}

// checkForeignKeyMetadataVisibility fails closed when foreign-key discovery cannot be
// proven complete: an unseen constraint from another schema could cascade into rows that
// were never copied. The proof is the library's PROCESS-gated InnoDB registry read.
func checkForeignKeyMetadataVisibility(ctx context.Context, f *facts) error {
	result, err := f.foreignKeys(ctx, fkIncoming)
	if err != nil {
		return inspectionError("fk_metadata_visibility", err)
	}
	if result.PrimaryError != nil {
		f.cfg.Logger.Errorf("Foreign-key metadata primary-source failure retained after fallback: %v", result.PrimaryError)
	}
	_, visibility, err := partitionClosure(validations.CheckFKClosure(result, f.cfg.SourceDatabase, f.tables))
	if err != nil || len(visibility) == 0 {
		return err
	}
	return &Error{Check: "FK_COVERAGE_VISIBILITY_CHECK", Message: fmt.Sprintf(
		"foreign-key metadata completeness is not established (state: %s), so GoArchive cannot verify that "+
			"no foreign key in another schema references the job's tables (an external ON DELETE CASCADE "+
			"or SET NULL would delete or change rows that were never copied). %s",
		visibility[0], visibilityDiagnostic(result.DowngradeReason))}
}

func visibilityDiagnostic(reason validations.ForeignKeyDowngradeReason) string {
	switch reason {
	case validations.ForeignKeyDowngradePrimaryQueryError:
		return "The primary InnoDB metadata query failed before rows were available. This can be caused by " +
			"privileges, connectivity, server state, or another query error; inspect the error log first. " +
			"If MySQL rejected the query for privilege, grant it with GRANT PROCESS ON *.* TO <user>; a " +
			"role-held PROCESS is equally acceptable"
	case validations.ForeignKeyDowngradePrimaryReadError:
		return "The primary InnoDB metadata query started, but its rows could not be read or decoded. " +
			"Changing privileges will not repair this read-stage failure; inspect the error log and verify " +
			"MySQL and dbsgomysql compatibility"
	default:
		return fmt.Sprintf("The validation library did not report a recognized primary-source downgrade cause "+
			"(reported reason: %s); inspect the error log and verify dbsgomysql compatibility before retrying", reason)
	}
}

// checkForeignKeyCoverage refuses a table outside the job that holds a foreign key into a
// job table, whatever its ON DELETE rule: its rows would block the delete, cascade, or be
// set to NULL.
func checkForeignKeyCoverage(ctx context.Context, f *facts) error {
	result, err := f.foreignKeys(ctx, fkIncoming)
	if err != nil {
		return inspectionError("fk_coverage", err)
	}
	external, _, err := partitionClosure(validations.CheckFKClosure(result, f.cfg.SourceDatabase, f.tables))
	if err != nil || len(external) == 0 {
		return err
	}
	byParent := map[string][]string{}
	for _, fk := range external {
		byParent[fk.ParentTable] = append(byParent[fk.ParentTable],
			fmt.Sprintf("%s.%s (ON DELETE %s)", fk.ChildSchema, fk.ChildTable, fk.OnDelete))
	}
	var lines []string
	for parent, children := range byParent {
		lines = append(lines, fmt.Sprintf("  - %s is referenced by: %v", parent, children))
	}
	sort.Strings(lines)
	return &Error{Check: "FK_COVERAGE_CHECK", Message: fmt.Sprintf(
		"Foreign key constraints not covered by relations (fatal for any ON DELETE rule):\n%s", strings.Join(lines, "\n"))}
}
