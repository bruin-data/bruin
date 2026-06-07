package polymarket

import "net/url"

type Config struct {
	Order            string `yaml:"order,omitempty" json:"order,omitempty" mapstructure:"order"`
	Ascending        string `yaml:"ascending,omitempty" json:"ascending,omitempty" mapstructure:"ascending"`
	Slug             string `yaml:"slug,omitempty" json:"slug,omitempty" mapstructure:"slug"`
	Closed           string `yaml:"closed,omitempty" json:"closed,omitempty" mapstructure:"closed"`
	Live             string `yaml:"live,omitempty" json:"live,omitempty" mapstructure:"live"`
	Active           string `yaml:"active,omitempty" json:"active,omitempty" mapstructure:"active"`
	Archived         string `yaml:"archived,omitempty" json:"archived,omitempty" mapstructure:"archived"`
	Featured         string `yaml:"featured,omitempty" json:"featured,omitempty" mapstructure:"featured"`
	TagID            string `yaml:"tag_id,omitempty" json:"tag_id,omitempty" mapstructure:"tag_id"`
	TagSlug          string `yaml:"tag_slug,omitempty" json:"tag_slug,omitempty" mapstructure:"tag_slug"`
	SeriesID         string `yaml:"series_id,omitempty" json:"series_id,omitempty" mapstructure:"series_id"`
	IncludeChat      string `yaml:"include_chat,omitempty" json:"include_chat,omitempty" mapstructure:"include_chat"`
	IncludeTemplate  string `yaml:"include_template,omitempty" json:"include_template,omitempty" mapstructure:"include_template"`
	IncludeMarkets   string `yaml:"include_markets,omitempty" json:"include_markets,omitempty" mapstructure:"include_markets"`
	ClobTokenIDs     string `yaml:"clob_token_ids,omitempty" json:"clob_token_ids,omitempty" mapstructure:"clob_token_ids"`
	ConditionIDs     string `yaml:"condition_ids,omitempty" json:"condition_ids,omitempty" mapstructure:"condition_ids"`
	QuestionIDs      string `yaml:"question_ids,omitempty" json:"question_ids,omitempty" mapstructure:"question_ids"`
	RelatedTags      string `yaml:"related_tags,omitempty" json:"related_tags,omitempty" mapstructure:"related_tags"`
	IncludeTag       string `yaml:"include_tag,omitempty" json:"include_tag,omitempty" mapstructure:"include_tag"`
	RFQEnabled       string `yaml:"rfq_enabled,omitempty" json:"rfq_enabled,omitempty" mapstructure:"rfq_enabled"`
	Limit            string `yaml:"limit,omitempty" json:"limit,omitempty" mapstructure:"limit"`
	Offset           string `yaml:"offset,omitempty" json:"offset,omitempty" mapstructure:"offset"`
	ParentEntityID   string `yaml:"parent_entity_id,omitempty" json:"parent_entity_id,omitempty" mapstructure:"parent_entity_id"`
	ParentEntityType string `yaml:"parent_entity_type,omitempty" json:"parent_entity_type,omitempty" mapstructure:"parent_entity_type"`
	Market           string `yaml:"market,omitempty" json:"market,omitempty" mapstructure:"market"`
	User             string `yaml:"user,omitempty" json:"user,omitempty" mapstructure:"user"`
	Q                string `yaml:"q,omitempty" json:"q,omitempty" mapstructure:"q"`
	EventsStatus     string `yaml:"events_status,omitempty" json:"events_status,omitempty" mapstructure:"events_status"`
	MarketsStatus    string `yaml:"markets_status,omitempty" json:"markets_status,omitempty" mapstructure:"markets_status"`
	TokenID          string `yaml:"token_id,omitempty" json:"token_id,omitempty" mapstructure:"token_id"`
	Side             string `yaml:"side,omitempty" json:"side,omitempty" mapstructure:"side"`
	Interval         string `yaml:"interval,omitempty" json:"interval,omitempty" mapstructure:"interval"`
	Fidelity         string `yaml:"fidelity,omitempty" json:"fidelity,omitempty" mapstructure:"fidelity"`
	TakerOnly        string `yaml:"taker_only,omitempty" json:"taker_only,omitempty" mapstructure:"taker_only"`
	FilterType       string `yaml:"filter_type,omitempty" json:"filter_type,omitempty" mapstructure:"filter_type"`
	FilterAmount     string `yaml:"filter_amount,omitempty" json:"filter_amount,omitempty" mapstructure:"filter_amount"`
	EventID          string `yaml:"event_id,omitempty" json:"event_id,omitempty" mapstructure:"event_id"`
	Type             string `yaml:"type,omitempty" json:"type,omitempty" mapstructure:"type"`
}

func (c Config) GetIngestrURI() string {
	q := url.Values{}
	setIfNotEmpty(q, "order", c.Order)
	setIfNotEmpty(q, "ascending", c.Ascending)
	setIfNotEmpty(q, "slug", c.Slug)
	setIfNotEmpty(q, "closed", c.Closed)
	setIfNotEmpty(q, "live", c.Live)
	setIfNotEmpty(q, "active", c.Active)
	setIfNotEmpty(q, "archived", c.Archived)
	setIfNotEmpty(q, "featured", c.Featured)
	setIfNotEmpty(q, "tag_id", c.TagID)
	setIfNotEmpty(q, "tag_slug", c.TagSlug)
	setIfNotEmpty(q, "series_id", c.SeriesID)
	setIfNotEmpty(q, "include_chat", c.IncludeChat)
	setIfNotEmpty(q, "include_template", c.IncludeTemplate)
	setIfNotEmpty(q, "include_markets", c.IncludeMarkets)
	setIfNotEmpty(q, "clob_token_ids", c.ClobTokenIDs)
	setIfNotEmpty(q, "condition_ids", c.ConditionIDs)
	setIfNotEmpty(q, "question_ids", c.QuestionIDs)
	setIfNotEmpty(q, "related_tags", c.RelatedTags)
	setIfNotEmpty(q, "include_tag", c.IncludeTag)
	setIfNotEmpty(q, "rfq_enabled", c.RFQEnabled)
	setIfNotEmpty(q, "limit", c.Limit)
	setIfNotEmpty(q, "offset", c.Offset)
	setIfNotEmpty(q, "parent_entity_id", c.ParentEntityID)
	setIfNotEmpty(q, "parent_entity_type", c.ParentEntityType)
	setIfNotEmpty(q, "market", c.Market)
	setIfNotEmpty(q, "user", c.User)
	setIfNotEmpty(q, "q", c.Q)
	setIfNotEmpty(q, "events_status", c.EventsStatus)
	setIfNotEmpty(q, "markets_status", c.MarketsStatus)
	setIfNotEmpty(q, "token_id", c.TokenID)
	setIfNotEmpty(q, "side", c.Side)
	setIfNotEmpty(q, "interval", c.Interval)
	setIfNotEmpty(q, "fidelity", c.Fidelity)
	setIfNotEmpty(q, "takerOnly", c.TakerOnly)
	setIfNotEmpty(q, "filterType", c.FilterType)
	setIfNotEmpty(q, "filterAmount", c.FilterAmount)
	setIfNotEmpty(q, "eventId", c.EventID)
	setIfNotEmpty(q, "type", c.Type)

	encoded := q.Encode()
	if encoded == "" {
		return "polymarket://"
	}
	return "polymarket://?" + encoded
}

func setIfNotEmpty(q url.Values, key, value string) {
	if value != "" {
		q.Set(key, value)
	}
}
