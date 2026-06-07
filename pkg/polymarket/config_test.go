package polymarket

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI_WithoutParams(t *testing.T) {
	t.Parallel()

	require.Equal(t, "polymarket://", Config{}.GetIngestrURI())
}

func TestConfig_GetIngestrURI_WithParams(t *testing.T) {
	t.Parallel()

	uri := Config{
		TokenID: "0xclob token/id",
		User:    "0xwallet address",
	}.GetIngestrURI()

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	require.Equal(t, "polymarket", parsed.Scheme)
	require.Empty(t, parsed.Host)
	require.Equal(t, "0xclob token/id", parsed.Query().Get("token_id"))
	require.Equal(t, "0xwallet address", parsed.Query().Get("user"))
}

func TestConfig_GetIngestrURI_MapsAllSupportedParams(t *testing.T) {
	t.Parallel()

	uri := Config{
		Order:            "createdAt",
		Ascending:        "true",
		Slug:             "sample-event",
		Closed:           "false",
		Live:             "true",
		Active:           "true",
		Archived:         "false",
		Featured:         "true",
		TagID:            "2",
		TagSlug:          "politics",
		SeriesID:         "7",
		IncludeChat:      "false",
		IncludeTemplate:  "true",
		IncludeMarkets:   "true",
		ClobTokenIDs:     "token-1,token-2",
		ConditionIDs:     "condition-1",
		QuestionIDs:      "question-1",
		RelatedTags:      "true",
		IncludeTag:       "true",
		RFQEnabled:       "false",
		Limit:            "100",
		Offset:           "10",
		ParentEntityID:   "123",
		ParentEntityType: "Event",
		Market:           "market-1",
		User:             "0xwallet",
		Q:                "election",
		EventsStatus:     "active",
		MarketsStatus:    "open",
		TokenID:          "token-1",
		Side:             "BUY",
		Interval:         "1d",
		Fidelity:         "60",
		TakerOnly:        "true",
		FilterType:       "CASH",
		FilterAmount:     "100",
		EventID:          "456",
		Type:             "TRADE",
	}.GetIngestrURI()

	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	query := parsed.Query()
	want := map[string]string{
		"order":              "createdAt",
		"ascending":          "true",
		"slug":               "sample-event",
		"closed":             "false",
		"live":               "true",
		"active":             "true",
		"archived":           "false",
		"featured":           "true",
		"tag_id":             "2",
		"tag_slug":           "politics",
		"series_id":          "7",
		"include_chat":       "false",
		"include_template":   "true",
		"include_markets":    "true",
		"clob_token_ids":     "token-1,token-2",
		"condition_ids":      "condition-1",
		"question_ids":       "question-1",
		"related_tags":       "true",
		"include_tag":        "true",
		"rfq_enabled":        "false",
		"limit":              "100",
		"offset":             "10",
		"parent_entity_id":   "123",
		"parent_entity_type": "Event",
		"market":             "market-1",
		"user":               "0xwallet",
		"q":                  "election",
		"events_status":      "active",
		"markets_status":     "open",
		"token_id":           "token-1",
		"side":               "BUY",
		"interval":           "1d",
		"fidelity":           "60",
		"takerOnly":          "true",
		"filterType":         "CASH",
		"filterAmount":       "100",
		"eventId":            "456",
		"type":               "TRADE",
	}

	require.Len(t, query, len(want))
	for key, value := range want {
		require.Equal(t, value, query.Get(key), key)
	}
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{User: "0xwallet"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "polymarket://?user=0xwallet", uri)
}
