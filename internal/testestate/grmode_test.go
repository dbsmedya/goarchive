package testestate

import (
	"slices"
	"testing"
)

// TestPersistedRestoreStatements pins the statements the mode helper's restore issues for each
// difference between a member's captured persisted rows and its current ones. The baselines are the
// two dbsgomysql v1.6.0 found: 8.0 persists the mode and the checks, 8.4 and 9.7 persist neither;
// and the switch functions persist both on every member they reach.
func TestPersistedRestoreStatements(t *testing.T) {
	on := PersistedRow{Present: true, Value: "ON"}
	off := PersistedRow{Present: true, Value: "OFF"}
	absent := PersistedRow{}
	eventual := PersistedRow{Present: true, Value: "EVENTUAL"}
	bopf := PersistedRow{Present: true, Value: "BEFORE_ON_PRIMARY_FAILOVER"}

	cases := []struct {
		name              string
		baseline, current map[string]PersistedRow
		want              []string
	}{
		{
			name: "8.0 member after a switch back: nothing to do",
			baseline: map[string]PersistedRow{
				VarGroupConsistency: eventual, VarEnforceUpdateChecks: off, VarSinglePrimaryMode: on,
			},
			current: map[string]PersistedRow{
				VarGroupConsistency: eventual, VarEnforceUpdateChecks: off, VarSinglePrimaryMode: on,
			},
		},
		{
			name: "8.4 member after a switch back: both rows the switch added are reset",
			baseline: map[string]PersistedRow{
				VarGroupConsistency: bopf, VarEnforceUpdateChecks: absent, VarSinglePrimaryMode: absent,
			},
			current: map[string]PersistedRow{
				VarGroupConsistency: bopf, VarEnforceUpdateChecks: off, VarSinglePrimaryMode: on,
			},
			want: []string{
				"RESET PERSIST IF EXISTS " + VarEnforceUpdateChecks,
				"RESET PERSIST IF EXISTS " + VarSinglePrimaryMode,
			},
		},
		{
			name: "a row left with another value, and a row lost, are persisted again",
			baseline: map[string]PersistedRow{
				VarGroupConsistency: eventual, VarEnforceUpdateChecks: off, VarSinglePrimaryMode: on,
			},
			current: map[string]PersistedRow{
				VarGroupConsistency: absent, VarEnforceUpdateChecks: on, VarSinglePrimaryMode: off,
			},
			want: []string{
				"SET PERSIST_ONLY " + VarGroupConsistency + " = 'EVENTUAL'",
				"SET PERSIST_ONLY " + VarEnforceUpdateChecks + " = 'OFF'",
				"SET PERSIST_ONLY " + VarSinglePrimaryMode + " = 'ON'",
			},
		},
		{
			name:     "a decimal value is persisted unquoted",
			baseline: map[string]PersistedRow{"group_replication_transaction_size_limit": {Present: true, Value: "150000000"}},
			current:  map[string]PersistedRow{},
			want:     []string{"SET PERSIST_ONLY group_replication_transaction_size_limit = 150000000"},
		},
		{
			name:     "a variable only the current state names is reset",
			baseline: map[string]PersistedRow{},
			current:  map[string]PersistedRow{VarSinglePrimaryMode: off},
			want:     []string{"RESET PERSIST IF EXISTS " + VarSinglePrimaryMode},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := PersistedRestoreStatements(testCase.baseline, testCase.current)
			if !slices.Equal(got, testCase.want) {
				t.Errorf("PersistedRestoreStatements = %q, want %q", got, testCase.want)
			}
		})
	}
}
