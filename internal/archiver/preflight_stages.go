package archiver

import "context"

// preflightStage is one ordered GoArchive safety decision. Applicability is
// calculated when a run is built; execution remains a simple ordered walk so a
// failure can never be bypassed or reported after a later stage.
type preflightStage struct {
	name    string
	enabled bool
	run     func(context.Context, *preflightRun) error
}

func executePreflightStages(ctx context.Context, run *preflightRun, stages []preflightStage) error {
	for _, stage := range stages {
		if !stage.enabled {
			continue
		}
		if err := stage.run(ctx, run); err != nil {
			return err
		}
	}
	return nil
}

// preflightStages is the single registry for stage order and profile
// applicability. Tests assert the enabled names for every command shape; adding,
// deleting, or reordering a stage therefore changes a focused unit verdict.
func (p *PreflightChecker) preflightStages(
	profile PreflightProfile,
	forceTriggers bool,
	enforceFKVisibility bool,
) []preflightStage {
	destinationConfigured := profile != PreflightProfileSourceOnly &&
		p.destinationDB != nil && p.destinationDBName != ""
	jobSchemaConfigured := p.destinationDB != nil && p.jobSchemaName != ""
	deleteCapable := profile == PreflightProfileFull || profile == PreflightProfileSourceOnly

	return []preflightStage{
		// The first six checks apply to every profile. Their order preserves the
		// earliest actionable structural failure before permissions or FK policy.
		{name: "ValidateTablesExist", enabled: true, run: p.ValidateTablesExist},
		{name: "ValidatePrimaryKeyColumns", enabled: true, run: p.ValidatePrimaryKeyColumns},
		{name: "ValidateSingleColumnPrimaryKey", enabled: true, run: p.ValidateSingleColumnPrimaryKey},
		{name: "ValidateRootPKNumeric", enabled: true, run: p.ValidateRootPKNumeric},
		{name: "ValidateStorageEngine", enabled: true, run: p.ValidateStorageEngine},

		// Tracking-schema permission is independent of destination data checks:
		// source-only commands still write job state when a job schema is configured.
		{name: "ValidateJobSchemaPermissions", enabled: jobSchemaConfigured, run: p.ValidateJobSchemaPermissions},
		{name: "ValidateDestinationTablesExist", enabled: destinationConfigured, run: p.ValidateDestinationTablesExist},
		{name: "ValidateDestinationSchemaCompatibility", enabled: destinationConfigured, run: p.ValidateDestinationSchemaCompatibility},
		{name: "ValidateDestinationWritePermissions", enabled: destinationConfigured, run: p.ValidateDestinationWritePermissions},
		{name: "ValidateDestinationInsertTriggers", enabled: destinationConfigured, run: p.ValidateDestinationInsertTriggers},

		// Visibility must fail closed before either FK coverage verdict relies on
		// the discovered set. Copy-only opts out because it never deletes source rows.
		{name: "ValidateForeignKeyIndexes", enabled: true, run: p.ValidateForeignKeyIndexes},
		{name: "ValidateForeignKeyMetadataVisibility", enabled: enforceFKVisibility, run: p.ValidateForeignKeyMetadataVisibility},
		{name: "ValidateForeignKeyCoverage", enabled: true, run: p.ValidateForeignKeyCoverage},
		{name: "ValidateInternalFKCoverage", enabled: true, run: p.ValidateInternalFKCoverage},

		// Every command reads source rows. Delete privileges, DELETE triggers and
		// cascade warnings follow only for profiles that can delete them.
		{name: "ValidateSourceSelectPermissions", enabled: true, run: p.ValidateSourceSelectPermissions},
		{name: "ValidateSourceDeletePermissions", enabled: deleteCapable, run: p.ValidateSourceDeletePermissions},
		{name: "ValidateTriggers", enabled: deleteCapable, run: func(ctx context.Context, run *preflightRun) error {
			return p.ValidateTriggers(ctx, run, forceTriggers)
		}},
		{name: "WarnCascadeRules", enabled: deleteCapable, run: p.WarnCascadeRules},
	}
}
