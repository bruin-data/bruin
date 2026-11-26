package cmd

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const teamRolesResponse = `{"roles":[{"key":"viewer","name":"Catalog User","description":"Reads the catalog.","custom":false,"permissions":["pipeline:list"]},` +
	`{"key":"custom-3","name":"On-call","description":"Copied from Catalog User.","custom":true,"permissions":["pipeline:list","pipeline:run:re-run"]}]}`

const teamMembersResponse = `{"members":[{"id":1,"name":"Owner Person","email":"owner@example.com","role":"owner","role_name":"Owner","owner":true,"bruin_employee":false,"expires_at":null},` +
	`{"id":7,"name":"Alex","email":"alex@example.com","role":null,"role_name":null,"owner":false,"bruin_employee":true,"expires_at":"2026-12-01T00:00:00Z"}]}`

func runTeams(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return runCloudCLI(t, append(append([]string{"teams"}, args...), "--api-key", "user-key")...)
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudTeamsRolesPrintsATableAndTheFullJSON(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /team/roles": teamRolesResponse})

	out, err := runTeams(t, "roles")
	require.NoError(t, err)
	assert.Contains(t, out, "On-call")
	assert.Contains(t, out, "custom-3")

	out, err = runTeams(t, "roles", "--output", "json")
	require.NoError(t, err)
	var roles []map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &roles))
	assert.Equal(t, []any{"pipeline:list", "pipeline:run:re-run"}, roles[1]["permissions"])
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudTeamsMembersPrintsRowsWithMissingFields(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /team/members": teamMembersResponse})

	out, err := runTeams(t, "members")
	require.NoError(t, err)
	assert.Contains(t, out, "owner@example.com")
	assert.Contains(t, out, "alex@example.com")
	assert.Contains(t, out, "2026-12-01T00:00:00Z")
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudTeamsMembersFailsWhenRefused(t *testing.T) {
	fakeDataAppAPI(t, map[string]string{"GET /team/members": `403|{"message":"Members can only be listed with a personal token, not a team token.","error":"forbidden"}`})

	_, err := runTeams(t, "members")
	require.Error(t, err)
}
