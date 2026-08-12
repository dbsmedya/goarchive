package archiver

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
	"github.com/dbsmedya/goarchive/internal/graph"
	"github.com/dbsmedya/goarchive/internal/logger"
)

func enabledPreflightStageNames(stages []preflightStage) []string {
	names := make([]string, 0, len(stages))
	for _, stage := range stages {
		if stage.enabled {
			names = append(names, stage.name)
		}
	}
	return names
}

func TestPreflightStageRegistry_ProfileMatrix(t *testing.T) {
	configured := &PreflightChecker{
		destinationDB:     new(sql.DB),
		destinationDBName: "destdb",
		jobSchemaName:     "jobs",
	}
	sourceOnly := &PreflightChecker{}

	all := []string{
		"ValidateTablesExist",
		"ValidatePrimaryKeyColumns",
		"ValidateSingleColumnPrimaryKey",
		"ValidateRootPKNumeric",
		"ValidateStorageEngine",
		"ValidateJobSchemaPermissions",
		"ValidateDestinationTablesExist",
		"ValidateDestinationSchemaCompatibility",
		"ValidateDestinationWritePermissions",
		"ValidateDestinationInsertTriggers",
		"ValidateForeignKeyIndexes",
		"ValidateForeignKeyMetadataVisibility",
		"ValidateForeignKeyCoverage",
		"ValidateInternalFKCoverage",
		"ValidateSourceSelectPermissions",
		"ValidateSourceDeletePermissions",
		"ValidateTriggers",
		"WarnCascadeRules",
	}

	tests := []struct {
		name                string
		checker             *PreflightChecker
		profile             PreflightProfile
		enforceFKVisibility bool
		want                []string
	}{
		{
			name:                "full",
			checker:             configured,
			profile:             PreflightProfileFull,
			enforceFKVisibility: true,
			want:                all,
		},
		{
			name:                "source-only keeps job schema but gates destination data",
			checker:             configured,
			profile:             PreflightProfileSourceOnly,
			enforceFKVisibility: true,
			want: []string{
				"ValidateTablesExist",
				"ValidatePrimaryKeyColumns",
				"ValidateSingleColumnPrimaryKey",
				"ValidateRootPKNumeric",
				"ValidateStorageEngine",
				"ValidateJobSchemaPermissions",
				"ValidateForeignKeyIndexes",
				"ValidateForeignKeyMetadataVisibility",
				"ValidateForeignKeyCoverage",
				"ValidateInternalFKCoverage",
				"ValidateSourceSelectPermissions",
				"ValidateSourceDeletePermissions",
				"ValidateTriggers",
				"WarnCascadeRules",
			},
		},
		{
			name:                "copy-only",
			checker:             configured,
			profile:             PreflightProfileNonDestructive,
			enforceFKVisibility: false,
			want: []string{
				"ValidateTablesExist",
				"ValidatePrimaryKeyColumns",
				"ValidateSingleColumnPrimaryKey",
				"ValidateRootPKNumeric",
				"ValidateStorageEngine",
				"ValidateJobSchemaPermissions",
				"ValidateDestinationTablesExist",
				"ValidateDestinationSchemaCompatibility",
				"ValidateDestinationWritePermissions",
				"ValidateDestinationInsertTriggers",
				"ValidateForeignKeyIndexes",
				"ValidateForeignKeyCoverage",
				"ValidateInternalFKCoverage",
				"ValidateSourceSelectPermissions",
			},
		},
		{
			name:                "non-destructive visibility enabled",
			checker:             configured,
			profile:             PreflightProfileNonDestructive,
			enforceFKVisibility: true,
			want:                append([]string(nil), all[:15]...),
		},
		{
			name:                "destination absent",
			checker:             sourceOnly,
			profile:             PreflightProfileFull,
			enforceFKVisibility: true,
			want: []string{
				"ValidateTablesExist",
				"ValidatePrimaryKeyColumns",
				"ValidateSingleColumnPrimaryKey",
				"ValidateRootPKNumeric",
				"ValidateStorageEngine",
				"ValidateForeignKeyIndexes",
				"ValidateForeignKeyMetadataVisibility",
				"ValidateForeignKeyCoverage",
				"ValidateInternalFKCoverage",
				"ValidateSourceSelectPermissions",
				"ValidateSourceDeletePermissions",
				"ValidateTriggers",
				"WarnCascadeRules",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := enabledPreflightStageNames(
				tt.checker.preflightStages(tt.profile, false, tt.enforceFKVisibility),
			)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("enabled stages:\n got %v\nwant %v", got, tt.want)
			}
		})
	}
}

func TestExecutePreflightStages_OrderAndDisabledStage(t *testing.T) {
	var recorded []string
	record := func(name string) func(context.Context, *preflightRun) error {
		return func(context.Context, *preflightRun) error {
			recorded = append(recorded, name)
			return nil
		}
	}
	stages := []preflightStage{
		{name: "first", enabled: true, run: record("first")},
		{name: "disabled", enabled: false, run: record("disabled")},
		{name: "second", enabled: true, run: record("second")},
		{name: "third", enabled: true, run: record("third")},
	}

	if err := executePreflightStages(context.Background(), &preflightRun{}, stages); err != nil {
		t.Fatalf("executePreflightStages: %v", err)
	}
	want := []string{"first", "second", "third"}
	if !reflect.DeepEqual(recorded, want) {
		t.Fatalf("recorded = %v, want %v", recorded, want)
	}
}

func TestExecutePreflightStages_FirstErrorStopsLaterStages(t *testing.T) {
	wantErr := errors.New("stage failed")
	var recorded []string
	stages := []preflightStage{
		{name: "first", enabled: true, run: func(context.Context, *preflightRun) error {
			recorded = append(recorded, "first")
			return nil
		}},
		{name: "failing", enabled: true, run: func(context.Context, *preflightRun) error {
			recorded = append(recorded, "failing")
			return wantErr
		}},
		{name: "later", enabled: true, run: func(context.Context, *preflightRun) error {
			recorded = append(recorded, "later")
			return nil
		}},
	}

	err := executePreflightStages(context.Background(), &preflightRun{}, stages)
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	wantRecorded := []string{"first", "failing"}
	if !reflect.DeepEqual(recorded, wantRecorded) {
		t.Fatalf("recorded = %v, want %v", recorded, wantRecorded)
	}
}

func TestPreflightStageRegistry_ForwardsForceTriggers(t *testing.T) {
	checker := &PreflightChecker{
		graph:  graph.NewGraph("orders", "id"),
		logger: logger.NewDefault(),
	}
	run := &preflightRun{
		srcDelTriggers:       []validations.TriggerInfo{{Table: "orders", Name: "before_delete", Event: "DELETE"}},
		srcDelTriggersLoaded: true,
		checker:              checker,
	}

	for _, force := range []bool{false, true} {
		stages := checker.preflightStages(PreflightProfileFull, force, false)
		var trigger preflightStage
		for _, stage := range stages {
			if stage.name == "ValidateTriggers" {
				trigger = stage
				break
			}
		}
		if trigger.run == nil {
			t.Fatal("ValidateTriggers stage not registered")
		}
		err := trigger.run(context.Background(), run)
		if force && err != nil {
			t.Fatalf("force=true: %v", err)
		}
		if !force && err == nil {
			t.Fatal("force=false: expected DELETE_TRIGGER_CHECK")
		}
	}
}
