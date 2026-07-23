// sources.go contains the registry of all available ingestr source tables.
// This data is used by the VS Code extension to list source tables for each source type.
// The registry serves as the single source of truth for ingestr source table metadata,
// eliminating the need to parse markdown documentation at runtime.
//
// To add a new source:
// 1. Add a new entry to SourceTablesRegistry with the source name as the key
// 2. Define all available tables with their PK, incremental key, and strategy
//
// To update existing source tables:
// 1. Find the source in SourceTablesRegistry
// 2. Update the table definitions as needed

package ingestr

import (
	"fmt"
	"sort"
)

// SourceTable represents a table available from an ingestr source with its metadata.
type SourceTable struct {
	Name        string `json:"name"`
	PrimaryKey  string `json:"primary_key,omitempty"`
	IncKey      string `json:"incremental_key,omitempty"`
	IncStrategy string `json:"incremental_strategy,omitempty"`
}

// Source represents an ingestr source with its available tables.
type Source struct {
	Name   string         `json:"name"`
	Tables []*SourceTable `json:"tables"`
}

var SourceTablesRegistry = map[string][]*SourceTable{
	// Azure Data Lake Storage Gen2 (user-defined paths)
	"adls": {},

	// Adapty - Subscription monetization platform
	"adapty": {
		{Name: "analytics?chart_id=<chart_id>", IncKey: "date", IncStrategy: "delete+insert"},
		{Name: "cohorts", IncKey: "date", IncStrategy: "delete+insert"},
		{Name: "conversion?from_period=<period>&to_period=<period>", IncKey: "date", IncStrategy: "delete+insert"},
		{Name: "funnel", IncKey: "date", IncStrategy: "delete+insert"},
		{Name: "ltv", IncKey: "date", IncStrategy: "delete+insert"},
		{Name: "retention", IncKey: "date", IncStrategy: "delete+insert"},
		{Name: "placements?placement_type=<placement_type>", IncStrategy: "replace"},
		{Name: "paywalls", PrimaryKey: "paywall_id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// Adjust - Mobile marketing analytics platform
	"adjust": {
		{Name: "campaigns", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "creatives", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "events", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "custom", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
	},

	// Airtable - Cloud-based spreadsheet/database platform
	"airtable": {
		{Name: "<base_id>/<table_name>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Allium - Blockchain data platform
	"allium": {
		{Name: "query:<query_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Anthropic - AI company (Claude API)
	"anthropic": {
		{Name: "claude_code_usage", PrimaryKey: "", IncKey: "date", IncStrategy: "merge"},
		{Name: "usage_report", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "cost_report", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "organization", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "workspaces", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "api_keys", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "invites", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "workspace_members", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// AppLovin - Mobile advertising platform
	"applovin": {
		{Name: "publisher-report", PrimaryKey: "day", IncKey: "day", IncStrategy: "merge"},
		{Name: "advertiser-report", PrimaryKey: "day", IncKey: "day", IncStrategy: "merge"},
		{Name: "advertiser-probabilistic-report", PrimaryKey: "day", IncKey: "day", IncStrategy: "merge"},
		{Name: "advertiser-ska-report", PrimaryKey: "day", IncKey: "day", IncStrategy: "merge"},
	},

	// AppLovin Max - Ad revenue optimization
	"applovinmax": {
		{Name: "user_ad_revenue", PrimaryKey: "partition_date", IncKey: "partition_date", IncStrategy: "merge"},
	},

	// Appsflyer - Mobile marketing analytics
	"appsflyer": {
		{Name: "campaigns", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "creatives", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// API-Football - Soccer data from API-SPORTS (api-sports.io), supports any league/season
	"apifootball": {
		{Name: "teams", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "stadiums", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "group_standings", PrimaryKey: "league_id, season, group_name, team_id", IncKey: "", IncStrategy: "merge"},
		{Name: "matches", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "players", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_events", PrimaryKey: "event_key", IncKey: "", IncStrategy: "merge"},
	},

	// football-data.org - FIFA World Cup soccer data
	"footballdata": {
		{Name: "teams", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "stadiums", PrimaryKey: "venue_key", IncKey: "", IncStrategy: "replace"},
		{Name: "group_standings", PrimaryKey: "competition_id, season_id, stage, standing_type, group_name, team_id", IncKey: "", IncStrategy: "replace"},
		{Name: "matches", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "players", PrimaryKey: "team_id, id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_events", PrimaryKey: "event_key", IncKey: "", IncStrategy: "merge"},
	},

	// BallDontLie - FIFA World Cup data
	"balldontlie": {
		{Name: "teams", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "stadiums", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "group_standings", PrimaryKey: "season_year, team_id", IncKey: "", IncStrategy: "replace"},
		{Name: "matches", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "players", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "rosters", PrimaryKey: "season_year, team_id, player_id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_lineups", PrimaryKey: "match_id, team_id, player_id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_events", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "player_match_stats", PrimaryKey: "match_id, player_id", IncKey: "", IncStrategy: "replace"},
		{Name: "team_match_stats", PrimaryKey: "match_id, team_id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_shots", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_momentum", PrimaryKey: "match_id, minute", IncKey: "", IncStrategy: "replace"},
		{Name: "match_best_players", PrimaryKey: "match_id, player_id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_avg_positions", PrimaryKey: "match_id, player_id", IncKey: "", IncStrategy: "replace"},
		{Name: "match_team_form", PrimaryKey: "match_id, team_id", IncKey: "", IncStrategy: "replace"},
	},

	// Apple Ads - Apple Search Ads campaign management
	"appleads": {
		{Name: "campaigns", PrimaryKey: "orgId,id", IncKey: "modificationTime", IncStrategy: "merge"},
		{Name: "ad_groups", PrimaryKey: "orgId,id", IncKey: "modificationTime", IncStrategy: "merge"},
		{Name: "ads", PrimaryKey: "orgId,id", IncKey: "modificationTime", IncStrategy: "merge"},
		{Name: "creatives", PrimaryKey: "orgId,id", IncKey: "modificationTime", IncStrategy: "merge"},
	},

	// Apple AppStore - App marketplace
	"appstore": {
		{Name: "app-downloads-detailed", PrimaryKey: "", IncKey: "processing_date", IncStrategy: "merge"},
		{Name: "app-store-discovery-and-engagement-detailed", PrimaryKey: "", IncKey: "processing_date", IncStrategy: "merge"},
		{Name: "app-sessions-detailed", PrimaryKey: "", IncKey: "processing_date", IncStrategy: "merge"},
		{Name: "app-store-installation-and-deletion-detailed", PrimaryKey: "", IncKey: "processing_date", IncStrategy: "merge"},
		{Name: "app-store-purchases-detailed", PrimaryKey: "", IncKey: "processing_date", IncStrategy: "merge"},
		{Name: "app-crashes-expanded", PrimaryKey: "", IncKey: "processing_date", IncStrategy: "merge"},
	},

	// Asana - Project management platform
	"asana": {
		{Name: "workspaces", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "projects", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "sections", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "tags", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "tasks", PrimaryKey: "gid", IncKey: "modified_at", IncStrategy: "merge"},
		{Name: "stories", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "teams", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Attio - AI-native CRM platform
	"attio": {
		{Name: "objects", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "records:{object_api_slug}", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "lists", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "list_entries:{list_id}", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "all_list_entries:{object_api_slug}", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Bruin Cloud - Data platform
	"bruin": {
		{Name: "pipelines", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "assets", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Chess.com - Online chess platform
	"chess": {
		{Name: "profiles", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "games", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "archives", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// PostHog - Product analytics platform
	"posthog": {
		{Name: "persons", PrimaryKey: "id", IncKey: "last_seen_at", IncStrategy: "merge"},
		{Name: "feature_flags", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "events", PrimaryKey: "id", IncKey: "timestamp", IncStrategy: "append"},
		{Name: "cohorts", PrimaryKey: "id", IncKey: "last_calculation", IncStrategy: "merge"},
		{Name: "event_definitions", PrimaryKey: "id", IncKey: "last_updated_at", IncStrategy: "merge"},
		{Name: "property_definitions:event", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "property_definitions:person", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "property_definitions:session", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "annotations", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// ClickUp - Productivity platform
	"clickup": {
		{Name: "user", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "teams", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "spaces", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "lists", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "tasks", PrimaryKey: "id", IncKey: "date_updated", IncStrategy: "merge"},
	},

	// Couchbase - NoSQL database (user-defined tables)
	"couchbase": {},

	// Cassandra - Wide-column database (user-defined tables)
	"cassandra": {},

	// CrateDB - Distributed SQL database (user-defined tables)
	"cratedb": {},

	// CSV - Local CSV files (user-defined paths)
	"csv": {},

	// JobTread - Construction management platform
	"jobtread": {
		{Name: "accounts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "jobs", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "contacts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "documents", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "tasks", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "cost_codes", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "cost_types", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "cost_items", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "locations", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "custom_fields", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "daily_logs", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "time_entries", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "files", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "comments", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "document_payments", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "cost_groups", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "events", PrimaryKey: "id", IncKey: "createdAt", IncStrategy: "merge"},
	},

	// Cursor - AI-powered code editor
	"cursor": {
		{Name: "team_members", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "daily_usage_data", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "team_spend", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "filtered_usage_events", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Customer.io - Customer engagement platform
	"customerio": {
		{Name: "activities", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "broadcasts", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "broadcast_actions", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "broadcast_action_metrics:period", PrimaryKey: "broadcast_id, action_id, period, step_index", IncKey: "", IncStrategy: "replace"},
		{Name: "broadcast_messages", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "broadcast_metrics:period", PrimaryKey: "broadcast_id, period, step_index", IncKey: "", IncStrategy: "replace"},
		{Name: "campaigns", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "campaign_actions", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "campaign_action_metrics:period", PrimaryKey: "campaign_id, action_id, period, step_index", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign_messages", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "campaign_metrics:period", PrimaryKey: "campaign_id, period, step_index", IncKey: "", IncStrategy: "replace"},
		{Name: "collections", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "customers", PrimaryKey: "cio_id", IncKey: "", IncStrategy: "replace"},
		{Name: "customer_activities", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "customer_attributes", PrimaryKey: "customer_id", IncKey: "", IncStrategy: "replace"},
		{Name: "customer_messages", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "customer_relationships", PrimaryKey: "customer_id, object_type_id, object_id", IncKey: "", IncStrategy: "replace"},
		{Name: "exports", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "info_ip_addresses", PrimaryKey: "ip", IncKey: "", IncStrategy: "replace"},
		{Name: "messages", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "newsletters", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "newsletter_metrics:period", PrimaryKey: "newsletter_id, period, step_index", IncKey: "", IncStrategy: "replace"},
		{Name: "newsletter_test_groups", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "object_types", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "objects", PrimaryKey: "object_type_id, object_id", IncKey: "", IncStrategy: "replace"},
		{Name: "reporting_webhooks", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "segments", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "sender_identities", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "subscription_topics", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "transactional_messages", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "workspaces", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
	},

	// IBM DB2 - Enterprise database (user-defined tables)
	"db2": {},

	// Docebo - Learning management system
	"docebo": {
		{Name: "branches", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "categories", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "certifications", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "course_enrollments", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "course_fields", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "course_learning_objects", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "courses", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "external_training", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "group_members", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "groups", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "learning_plan_course_enrollments", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "learning_plan_enrollments", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "learning_plans", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "sessions", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "user_fields", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// DynamoDB - AWS NoSQL database (user-defined tables)
	"dynamodb": {},

	// Dune - Blockchain analytics platform
	"dune": {
		{Name: "queries"},
		{Name: "query:<id>"},
		{Name: "query:<id>:<params>"},
		{Name: "sql:<raw SQL>"},
	},

	// Elasticsearch - Search and analytics engine (user-defined indices)
	"elasticsearch": {},

	// ESPN - Public sports data (auth-less site API)
	"espn": {
		{Name: "teams", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "scoreboard", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "competitors", PrimaryKey: "event_id, competition_id, team_id", IncKey: "", IncStrategy: "merge"},
		{Name: "standings", PrimaryKey: "league_id, group_id, season, team_id", IncKey: "", IncStrategy: "replace"},
		{Name: "news", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
	},

	// Facebook Ads - Advertising platform
	"facebookads": {
		{Name: "campaigns", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "ad_sets", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "ads", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "ad_creatives", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "leads", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "facebook_insights", PrimaryKey: "date_start", IncKey: "date_start", IncStrategy: "merge"},
	},

	// Fireflies - AI meeting assistant
	"fireflies": {
		{Name: "active_meetings", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "analytics", PrimaryKey: "", IncKey: "end_time", IncStrategy: "merge"},
		{Name: "channels", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "user_groups", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "transcripts", PrimaryKey: "", IncKey: "date", IncStrategy: "merge"},
		{Name: "bites", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "contacts", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Trello - Project management (boards, lists, cards)
	"trello": {
		{Name: "boards", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "organizations", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "lists", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "members", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "labels", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "checklists", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "cards", PrimaryKey: "id", IncKey: "dateLastActivity", IncStrategy: "merge"},
		{Name: "actions", PrimaryKey: "id", IncKey: "date", IncStrategy: "merge"},
	},

	// Fluxx - Grants management platform
	"fluxx": {
		{Name: "claim", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "grant_request", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "organization", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "program", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "request_transaction", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "user", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// Frankfurter - Exchange rates API
	"frankfurter": {
		{Name: "currencies", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "latest", PrimaryKey: "date,currency_code,base_currency", IncKey: "", IncStrategy: "merge"},
		{Name: "exchange_rates", PrimaryKey: "date,currency_code,base_currency", IncKey: "date", IncStrategy: "merge"},
	},

	// Freshdesk - Customer service platform
	"freshdesk": {
		{Name: "agents", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "companies", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "contacts", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "groups", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "roles", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "tickets", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// FundraiseUp - Donation platform
	"fundraiseup": {
		{Name: "donations", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "events", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "fundraisers", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "recurring_plans", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "supporters", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Google Cloud Storage (user-defined paths)
	"gcs": {},

	// GitHub - Developer platform
	"github": {
		{Name: "issues", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "pull_requests", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "repo_events", PrimaryKey: "id", IncKey: "created_at", IncStrategy: "merge"},
		{Name: "stargazers", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Google Analytics
	"googleanalytics": {
		{Name: "custom:<dimensions>:<metrics>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Google Search Console
	"gsc": {
		{Name: "<granularity>:<dimensions>", PrimaryKey: "", IncKey: "date", IncStrategy: "merge"},
		{Name: "searchAppearance", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "sites", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "sitemaps", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Google Sheets
	"google_sheets": {
		{Name: "<spreadsheet_id>.<sheet_name>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Google Ads
	"googleads": {
		{Name: "account_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "campaign_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "ad_group_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "ad_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "audience_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "keyword_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "click_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "landing_page_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "search_keyword_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
		{Name: "search_term_report_daily", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
	},

	// Gorgias - E-commerce helpdesk
	"gorgias": {
		{Name: "customers", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "tickets", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Granola - AI meeting notes
	"granola": {
		{Name: "notes", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "folders", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
	},

	// G2 - Software review platform
	"g2": {
		{Name: "products", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "my_products", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "vendors", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "categories", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "category_features", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "product_features", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "buyer_intent", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "competitors", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "discussions", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "downloads", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "integration_reviews", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "questions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "reviews", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "screenshots", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "videos", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// Hostaway - Property management system
	"hostaway": {
		{Name: "listings", PrimaryKey: "", IncKey: "latestActivityOn", IncStrategy: "merge"},
		{Name: "listing_fee_settings", PrimaryKey: "", IncKey: "updatedOn", IncStrategy: "merge"},
		{Name: "listing_pricing_settings", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "listing_agreements", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "listing_calendars", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "reservations", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "conversations", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// HTTP - Public file URLs (user-defined paths)
	"http": {},

	// HubSpot - CRM platform
	"hubspot": {
		{Name: "contacts"},
		{Name: "companies"},
		{Name: "deals"},
		{Name: "tickets"},
		{Name: "products"},
		{Name: "quotes"},
		{Name: "calls"},
		{Name: "emails"},
		{Name: "feedback_submissions"},
		{Name: "line_items"},
		{Name: "meetings"},
		{Name: "notes"},
		{Name: "tasks"},
		{Name: "carts"},
		{Name: "discounts"},
		{Name: "fees"},
		{Name: "invoices"},
		{Name: "commerce_payments"},
		{Name: "taxes"},
		{Name: "owners"},
		{Name: "schemas"},
		{Name: "pipelines"},
		{Name: "pipeline_stages"},
	},

	// Indeed - Job search platform
	"indeed": {
		{Name: "campaigns", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign_details", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign_budget", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign_jobs", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign_properties", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign_stats", PrimaryKey: "", IncKey: "Date", IncStrategy: "merge"},
		{Name: "account", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "traffic_stats", PrimaryKey: "", IncKey: "date", IncStrategy: "merge"},
	},

	// InfluxDB - Time series database (user-defined measurements)
	"influxdb": {},

	// Intercom - Customer messaging platform
	"intercom": {
		{Name: "contacts", PrimaryKey: "", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "companies", PrimaryKey: "", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "conversations", PrimaryKey: "", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "articles", PrimaryKey: "", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "tags", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "segments", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "teams", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "admins", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "data_attributes", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Internet Society Pulse
	"isoc_pulse": {
		{Name: "dnssec_adoption", PrimaryKey: "date", IncKey: "date", IncStrategy: "merge"},
		{Name: "dnssec_tld_adoption", PrimaryKey: "date", IncKey: "date", IncStrategy: "merge"},
		{Name: "dnssec_validation", PrimaryKey: "date", IncKey: "date", IncStrategy: "merge"},
		{Name: "https", PrimaryKey: "date", IncKey: "date", IncStrategy: "merge"},
		{Name: "ipv6", PrimaryKey: "date", IncKey: "date", IncStrategy: "merge"},
	},

	// Jira - Issue tracking
	"jira": {
		{Name: "projects", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "issues", PrimaryKey: "id", IncKey: "fields.updated", IncStrategy: "merge"},
		{Name: "users", PrimaryKey: "accountId", IncKey: "", IncStrategy: "replace"},
		{Name: "issue_types", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "statuses", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "priorities", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "resolutions", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "project_versions", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "project_components", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
	},

	// Kalshi - Prediction market exchange
	"kalshi": {
		{Name: "exchange_status", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "exchange_schedule", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "exchange_announcements", PrimaryKey: "id", IncKey: "created_time", IncStrategy: "merge"},
		{Name: "series", PrimaryKey: "ticker", IncKey: "updated_time", IncStrategy: "merge"},
		{Name: "series_by_ticker", PrimaryKey: "ticker", IncKey: "", IncStrategy: "merge"},
		{Name: "events", PrimaryKey: "event_ticker", IncKey: "updated_time", IncStrategy: "merge"},
		{Name: "event_by_ticker", PrimaryKey: "event_ticker", IncKey: "", IncStrategy: "merge"},
		{Name: "markets", PrimaryKey: "ticker", IncKey: "updated_time", IncStrategy: "merge"},
		{Name: "market_by_ticker", PrimaryKey: "ticker", IncKey: "", IncStrategy: "merge"},
		{Name: "market_orderbook", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "market_orderbooks", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "market_trades", PrimaryKey: "trade_id", IncKey: "created_time", IncStrategy: "merge"},
		{Name: "market_candlesticks", PrimaryKey: "end_period_ts", IncKey: "end_period_ts", IncStrategy: "merge"},
		{Name: "market_candlesticks_batch", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "historical_markets", PrimaryKey: "ticker", IncKey: "", IncStrategy: "merge"},
		{Name: "historical_trades", PrimaryKey: "trade_id", IncKey: "created_time", IncStrategy: "merge"},
	},

	// Kafka - Event streaming (user-defined topics)
	"kafka": {},

	// RabbitMQ - Message broker (user-defined queues)
	"rabbitmq": {},

	// Kinesis - AWS streaming (user-defined streams)
	"kinesis": {},

	// Klaviyo - Marketing automation
	"klaviyo": {
		{Name: "events", PrimaryKey: "id", IncKey: "datetime", IncStrategy: "merge"},
		{Name: "profiles", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "campaigns", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "metrics", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "tags", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "coupons", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "catalog-variants", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "catalog-categories", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "catalog-items", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "flows", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "lists", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "images", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "segments", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
		{Name: "forms", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "templates", PrimaryKey: "id", IncKey: "updated", IncStrategy: "merge"},
	},

	// Linear - Project management
	"linear": {
		{Name: "issues", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "users", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "workflow_states", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "cycles", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "attachments", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "comments", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "documents", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "labels", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "projects", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "teams", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "organization", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
	},

	// LinkedIn Ads
	"linkedinads": {
		{Name: "custom:<dimensions>:<metrics>", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
	},

	// Mailchimp - Email marketing
	"mailchimp": {
		{Name: "account", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "audiences", PrimaryKey: "id", IncKey: "date_created", IncStrategy: "merge"},
		{Name: "automations", PrimaryKey: "id", IncKey: "create_time", IncStrategy: "merge"},
		{Name: "campaigns", PrimaryKey: "id", IncKey: "create_time", IncStrategy: "merge"},
		{Name: "connected_sites", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "conversations", PrimaryKey: "id", IncKey: "last_message.timestamp", IncStrategy: "merge"},
		{Name: "ecommerce_stores", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "facebook_ads", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "landing_pages", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "reports", PrimaryKey: "id", IncKey: "send_time", IncStrategy: "merge"},
	},

	// Manifold - Prediction market platform
	"manifold": {
		{Name: "markets", PrimaryKey: "id", IncKey: "createdTime", IncStrategy: "merge"},
		{Name: "search_markets", PrimaryKey: "id", IncKey: "createdTime", IncStrategy: "merge"},
		{Name: "market_by_id", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "market_by_slug", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "market_probability", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "market_probabilities", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "market_positions", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "bets", PrimaryKey: "id", IncKey: "createdTime", IncStrategy: "merge"},
		{Name: "comments", PrimaryKey: "id", IncKey: "createdTime", IncStrategy: "merge"},
		{Name: "groups", PrimaryKey: "id", IncKey: "createdTime", IncStrategy: "merge"},
		{Name: "group_by_slug", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "group_by_id", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "user_by_username", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "user_by_id", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "user_portfolio", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "user_portfolio_history", PrimaryKey: "timestamp", IncKey: "timestamp", IncStrategy: "merge"},
		{Name: "user_contract_metrics", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "transactions", PrimaryKey: "id", IncKey: "createdTime", IncStrategy: "merge"},
		{Name: "leagues", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "boost_history", PrimaryKey: "id", IncKey: "createdTime", IncStrategy: "merge"},
	},

	// Amplitude - Analytics
	"amplitude": {
		{Name: "events", PrimaryKey: "uuid", IncKey: "event_time", IncStrategy: "merge"},
		{Name: "cohorts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "annotations", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "event_types", PrimaryKey: "event_type", IncKey: "", IncStrategy: "replace"},
		{Name: "event_categories", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "event_properties", PrimaryKey: "event_property", IncKey: "", IncStrategy: "replace"},
		{Name: "user_properties", PrimaryKey: "user_property", IncKey: "", IncStrategy: "replace"},
	},

	// Payrails - Payments
	"payrails": {
		{Name: "payments", PrimaryKey: "id", IncKey: "createdAt", IncStrategy: "merge"},
		{Name: "instruments", PrimaryKey: "id", IncKey: "createdAt", IncStrategy: "merge"},
		{Name: "executions", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
	},

	// FastSpring - Payments & Subscriptions
	"fastspring": {
		{Name: "orders", PrimaryKey: "id", IncKey: "changed", IncStrategy: "merge"},
		{Name: "subscriptions", PrimaryKey: "id", IncKey: "changed", IncStrategy: "merge"},
		{Name: "accounts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "products", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "coupons", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "subscription_report", PrimaryKey: "subscription_id, transaction_date", IncKey: "sync_date", IncStrategy: "merge"},
		{Name: "revenue_report", PrimaryKey: "order_id, transaction_date", IncKey: "syncdate", IncStrategy: "merge"},
	},

	// Mixpanel - Analytics
	"mixpanel": {
		{Name: "events", PrimaryKey: "distinct_id", IncKey: "time", IncStrategy: "merge"},
		{Name: "profiles", PrimaryKey: "distinct_id", IncKey: "last_seen", IncStrategy: "merge"},
	},

	// Monday.com - Work OS
	"monday": {
		{Name: "account", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "account_roles", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "boards", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "workspaces", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "webhooks", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "updates", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "teams", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "tags", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// MongoDB - NoSQL database (user-defined collections)
	"mongo": {},

	// MySQL - Relational database (user-defined tables)
	"mysql": {},

	// Vitess - MySQL-compatible sharded database (user-defined tables)
	"vitess": {},

	// PlanetScale MySQL - Managed Vitess platform (user-defined tables)
	"planetscale_mysql": {},

	// Notion - Workspace
	"notion": {
		{Name: "<database_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Personio - HR platform
	"personio": {
		{Name: "employees", PrimaryKey: "id", IncKey: "last_modified_at", IncStrategy: "merge"},
		{Name: "absence_types", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "absences", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "attendances", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "projects", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "document_categories", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "custom_reports_list", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "employees_absences_balance", PrimaryKey: "employee_id,id", IncKey: "", IncStrategy: "merge"},
	},

	// PhantomBuster - Web scraping
	"phantombuster": {
		{Name: "completed_phantoms:<agent_id>", PrimaryKey: "container_id", IncKey: "ended_at", IncStrategy: "merge"},
	},

	// Pinterest - Social media
	"pinterest": {
		{Name: "pins", PrimaryKey: "id", IncKey: "created_at", IncStrategy: "merge"},
		{Name: "boards", PrimaryKey: "id", IncKey: "created_at", IncStrategy: "merge"},
	},

	// Pipedrive - CRM
	"pipedrive": {
		{Name: "activities", PrimaryKey: "id", IncKey: "update_time", IncStrategy: "merge"},
		{Name: "deals", PrimaryKey: "id", IncKey: "update_time", IncStrategy: "merge"},
		{Name: "persons", PrimaryKey: "id", IncKey: "update_time", IncStrategy: "merge"},
		{Name: "organizations", PrimaryKey: "id", IncKey: "update_time", IncStrategy: "merge"},
		{Name: "products", PrimaryKey: "id", IncKey: "update_time", IncStrategy: "merge"},
		{Name: "users", PrimaryKey: "id", IncKey: "update_time", IncStrategy: "merge"},
	},

	// Polymarket - Prediction market platform
	"polymarket": {
		{Name: "events", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "markets", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "tags", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "series", PrimaryKey: "id", IncKey: "updatedAt", IncStrategy: "merge"},
		{Name: "comments", PrimaryKey: "id", IncKey: "createdAt", IncStrategy: "merge"},
		{Name: "search", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "orderbook", PrimaryKey: "asset_id", IncKey: "", IncStrategy: "merge"},
		{Name: "price", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "midpoint", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "spread", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "last_trade_price", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "price_history", PrimaryKey: "t", IncKey: "t", IncStrategy: "merge"},
		{Name: "trades", PrimaryKey: "transactionHash", IncKey: "timestamp", IncStrategy: "merge"},
		{Name: "positions", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "closed_positions", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "activity", PrimaryKey: "transactionHash", IncKey: "timestamp", IncStrategy: "merge"},
	},

	// Plus Vibe AI - Email marketing
	"plusvibeai": {
		{Name: "campaigns", PrimaryKey: "", IncKey: "modified_at", IncStrategy: "merge"},
		{Name: "leads", PrimaryKey: "", IncKey: "modified_at", IncStrategy: "merge"},
		{Name: "email_accounts", PrimaryKey: "", IncKey: "timestamp_updated", IncStrategy: "merge"},
		{Name: "emails", PrimaryKey: "", IncKey: "timestamp_created", IncStrategy: "merge"},
		{Name: "blocklist", PrimaryKey: "", IncKey: "created_at", IncStrategy: "merge"},
		{Name: "webhooks", PrimaryKey: "", IncKey: "modified_at", IncStrategy: "merge"},
		{Name: "tags", PrimaryKey: "", IncKey: "modified_at", IncStrategy: "merge"},
	},

	// Primer - Payments infrastructure
	"primer": {
		{Name: "payments", PrimaryKey: "id", IncKey: "dateUpdated", IncStrategy: "merge"},
	},

	// QuickBooks - Accounting
	"quickbooks": {
		{Name: "customers", PrimaryKey: "id", IncKey: "lastupdatedtime", IncStrategy: "merge"},
		{Name: "invoices", PrimaryKey: "id", IncKey: "lastupdatedtime", IncStrategy: "merge"},
		{Name: "accounts", PrimaryKey: "id", IncKey: "lastupdatedtime", IncStrategy: "merge"},
		{Name: "vendors", PrimaryKey: "id", IncKey: "lastupdatedtime", IncStrategy: "merge"},
		{Name: "payments", PrimaryKey: "id", IncKey: "lastupdatedtime", IncStrategy: "merge"},
	},

	// RevenueCat - Subscription management
	"revenuecat": {
		{Name: "projects", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "customers", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "products", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "entitlements", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "offerings", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
	},

	// Reddit Ads - Advertising platform
	"reddit_ads": {
		{Name: "accounts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "campaigns", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "ad_groups", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "ads", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "posts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "custom_audiences", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "saved_audiences", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "pixels", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "funding_instruments", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "custom", PrimaryKey: "level_id,breakdowns", IncKey: "date", IncStrategy: "merge"},
	},

	// Amazon S3 (user-defined paths)
	"s3": {},

	// Salesforce - CRM
	"salesforce": {
		{Name: "user", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "user_role", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "opportunity", PrimaryKey: "id", IncKey: "last_timestamp", IncStrategy: "merge"},
		{Name: "opportunity_line_item", PrimaryKey: "id", IncKey: "last_timestamp", IncStrategy: "merge"},
		{Name: "opportunity_contact_role", PrimaryKey: "id", IncKey: "last_timestamp", IncStrategy: "merge"},
		{Name: "account", PrimaryKey: "id", IncKey: "last_timestamp", IncStrategy: "merge"},
		{Name: "contact", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "lead", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "campaign_member", PrimaryKey: "id", IncKey: "last_timestamp", IncStrategy: "merge"},
		{Name: "product", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "pricebook", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "pricebook_entry", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "task", PrimaryKey: "id", IncKey: "last_timestamp", IncStrategy: "merge"},
		{Name: "event", PrimaryKey: "id", IncKey: "last_timestamp", IncStrategy: "merge"},
	},

	// SAP HANA (user-defined tables)
	"hana": {},

	// SendGrid - Email delivery and marketing platform
	"sendgrid": {
		{Name: "messages", PrimaryKey: "msg_id", IncKey: "last_event_time", IncStrategy: "merge"},
		{Name: "global_stats", PrimaryKey: "date", IncKey: "date", IncStrategy: "merge"},
		{Name: "bounces", PrimaryKey: "email, created", IncKey: "created", IncStrategy: "merge"},
		{Name: "blocks", PrimaryKey: "email, created", IncKey: "created", IncStrategy: "merge"},
		{Name: "invalid_emails", PrimaryKey: "email, created", IncKey: "created", IncStrategy: "merge"},
		{Name: "spam_reports", PrimaryKey: "email, created", IncKey: "created", IncStrategy: "merge"},
		{Name: "unsubscribes", PrimaryKey: "email, created", IncKey: "created", IncStrategy: "merge"},
		{Name: "suppression_groups", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "suppression_group_members", PrimaryKey: "group_id, email", IncKey: "", IncStrategy: "replace"},
		{Name: "templates", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "lists", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "single_sends", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},
	// Twilio - Cloud communications (messaging, voice, phone numbers)
	"twilio": {
		{Name: "messages", PrimaryKey: "sid", IncKey: "", IncStrategy: "replace"},
		{Name: "calls", PrimaryKey: "sid", IncKey: "date_updated", IncStrategy: "merge"},
		{Name: "recordings", PrimaryKey: "sid", IncKey: "date_updated", IncStrategy: "merge"},
		{Name: "incoming_phone_numbers", PrimaryKey: "sid", IncKey: "", IncStrategy: "replace"},
		{Name: "usage_records", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},
	// Braze - Customer engagement platform (campaigns, canvases, KPIs)
	"braze": {
		{Name: "campaigns", PrimaryKey: "id", IncKey: "last_edited", IncStrategy: "merge"},
		{Name: "campaign_series", PrimaryKey: "time, campaign_id", IncKey: "time", IncStrategy: "merge"},
		{Name: "canvases", PrimaryKey: "id", IncKey: "last_edited", IncStrategy: "merge"},
		{Name: "canvas_series", PrimaryKey: "time, canvas_id", IncKey: "time", IncStrategy: "merge"},
		{Name: "segments", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "segment_series", PrimaryKey: "time, segment_id", IncKey: "time", IncStrategy: "merge"},
		{Name: "events", PrimaryKey: "name", IncKey: "", IncStrategy: "replace"},
		{Name: "event_series", PrimaryKey: "time, event_name", IncKey: "time", IncStrategy: "merge"},
		{Name: "products", PrimaryKey: "product_id", IncKey: "", IncStrategy: "replace"},
		{Name: "sessions", PrimaryKey: "time", IncKey: "time", IncStrategy: "merge"},
		{Name: "purchase_quantity", PrimaryKey: "time", IncKey: "time", IncStrategy: "merge"},
		{Name: "purchase_revenue", PrimaryKey: "time", IncKey: "time", IncStrategy: "merge"},
		{Name: "kpi_dau", PrimaryKey: "time", IncKey: "time", IncStrategy: "merge"},
		{Name: "kpi_mau", PrimaryKey: "time", IncKey: "time", IncStrategy: "merge"},
		{Name: "kpi_new_users", PrimaryKey: "time", IncKey: "time", IncStrategy: "merge"},
		{Name: "kpi_uninstalls", PrimaryKey: "time", IncKey: "time", IncStrategy: "merge"},
		{Name: "user_data", PrimaryKey: "braze_id, segment_id", IncKey: "", IncStrategy: "replace"},
	},

	// SFTP (user-defined paths)
	"sftp": {},

	// SharePoint (user-defined document library paths)
	"sharepoint": {
		{Name: "<path/to/file.xlsx>", PrimaryKey: "_source_file,_sheet_name,_row_idx", IncKey: "", IncStrategy: "replace"},
		{Name: "<path/to/file.xlsx>#sheet=<sheet_name>", PrimaryKey: "_source_file,_sheet_name,_row_idx", IncKey: "", IncStrategy: "replace"},
		{Name: "<path/to/file.xlsx>#sheets=<sheet_a>|<sheet_b>", PrimaryKey: "_source_file,_sheet_name,_row_idx", IncKey: "", IncStrategy: "replace"},
		{Name: "<path/to/files/*.xlsx>#sheets=<sheet_a>|<sheet_b>", PrimaryKey: "_source_file,_sheet_name,_row_idx", IncKey: "", IncStrategy: "replace"},
		{Name: "<path/to/file.csv>#csv,encoding=utf-16le,sep=tab", PrimaryKey: "_source_file,_row_idx", IncKey: "", IncStrategy: "replace"},
	},

	// Shopify - E-commerce
	"shopify": {
		{Name: "orders", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "customers", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "discounts", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "products", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "inventory_items", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "transactions", PrimaryKey: "id", IncKey: "id", IncStrategy: "merge"},
		{Name: "balance", PrimaryKey: "currency", IncKey: "", IncStrategy: "merge"},
		{Name: "events", PrimaryKey: "id", IncKey: "created_at", IncStrategy: "merge"},
		{Name: "price_rules", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// Slack - Messaging platform
	"slack": {
		{Name: "channels", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "messages:<channel1>,<channel2>", PrimaryKey: "ts", IncKey: "ts", IncStrategy: "merge"},
		{Name: "access_logs", PrimaryKey: "user_id", IncKey: "", IncStrategy: "append"},
	},

	// Smartsheet
	"smartsheet": {
		{Name: "<sheet_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Snapchat Ads
	"snapchatads": {
		{Name: "organizations", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "fundingsources", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "billingcenters", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "adaccounts", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "campaigns", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "adsquads", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "ads", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "creatives", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "segments", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// Socrata - Open data platform
	"socrata": {
		{Name: "<dataset_id>", PrimaryKey: ":id", IncKey: "", IncStrategy: "replace"},
	},

	// Solidgate - Payments platform
	"solidgate": {
		{Name: "subscriptions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "apm_orders", PrimaryKey: "order_id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "card_orders", PrimaryKey: "order_id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "financial_entries", PrimaryKey: "id", IncKey: "created_at", IncStrategy: "merge"},
	},

	// Square - Payments and commerce platform
	"square": {
		{Name: "payments", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "refunds", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "orders", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "customers", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "catalog_objects", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "team_members", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "inventory", PrimaryKey: "catalog_object_id, location_id, state", IncKey: "calculated_at", IncStrategy: "merge"},
		{Name: "locations", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "team_member_wages", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "shifts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "bank_accounts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "cash_drawers", PrimaryKey: "id, location_id", IncKey: "", IncStrategy: "replace"},
		{Name: "loyalty", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
	},

	// GCP Spanner (user-defined tables)
	"spanner": {},

	// SQLite (user-defined tables)
	"sqlite": {},

	// Stripe - Payments
	"stripe": {
		{Name: "account", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "apple_pay_domain", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "application_fee", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "balance_transaction", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "charge", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "checkout_session", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "coupon", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "credit_note", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "customer", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "dispute", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "event", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "invoice", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "invoice_item", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "invoice_line_item", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "payment_intent", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "payment_link", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "payment_method", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "payment_method_domain", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "payout", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "plan", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "price", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "product", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "promotion_code", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "quote", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "refund", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "review", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "setup_attempt", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "setup_intent", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "shipping_rate", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "subscription", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "subscription_item", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "subscription_schedule", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "tax_code", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "tax_id", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "tax_rate", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "top_up", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "transfer", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "webhook_endpoint", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
	},

	// Paddle - Billing and subscriptions
	"paddle": {
		{Name: "customers", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "products", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "prices", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "discounts", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "transactions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "subscriptions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "adjustments", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// Chargebee - Subscription billing
	"chargebee": {
		{Name: "customers", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "subscriptions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "invoices", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "transactions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "orders", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "events", PrimaryKey: "id", IncKey: "occurred_at", IncStrategy: "merge"},
	},

	// Recurly - Subscription billing
	"recurly": {
		{Name: "accounts", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "subscriptions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "invoices", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "transactions", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "plans", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// GitLab - DevOps and code hosting
	"gitlab": {
		{Name: "projects", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "groups", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "issues", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "merge_requests", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
	},

	// SurveyMonkey - Survey and feedback platform
	"surveymonkey": {
		{Name: "surveys", PrimaryKey: "id", IncKey: "date_modified", IncStrategy: "merge"},
		{Name: "survey_details", PrimaryKey: "id", IncKey: "date_modified", IncStrategy: "merge"},
		{Name: "survey_responses", PrimaryKey: "id", IncKey: "date_modified", IncStrategy: "merge"},
		{Name: "collectors", PrimaryKey: "id", IncKey: "date_modified", IncStrategy: "merge"},
		{Name: "contact_lists", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "contacts", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
	},

	// Typeform - Online form and survey platform
	"typeform": {
		{Name: "forms", PrimaryKey: "id", IncKey: "last_updated_at", IncStrategy: "merge"},
		{Name: "responses", PrimaryKey: "response_id", IncKey: "submitted_at", IncStrategy: "merge"},
		{Name: "workspaces", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
		{Name: "themes", PrimaryKey: "id", IncKey: "", IncStrategy: "replace"},
	},

	// TikTok Ads
	"tiktokads": {
		{Name: "custom:<dimensions>:<metrics>", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
	},

	// Trustpilot - Reviews platform
	"trustpilot": {
		{Name: "reviews", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Trino - Distributed SQL query engine (user-defined tables)
	"trino": {},

	// StarRocks - OLAP database, incl. lakehouse catalogs (user-defined tables)
	"starrocks": {},

	// Wise - Money transfers
	"wise": {
		{Name: "profiles", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "transfers", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "balances", PrimaryKey: "id", IncKey: "modificationTime", IncStrategy: "merge"},
	},

	// Wistia - Video hosting and analytics
	"wistia": {
		{Name: "account", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "token", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "allowed_domains", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "folders", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "folder:<folder_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "folder_sharings:<folder_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "subfolders:<folder_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "medias", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "media:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "captions", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "captions:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "media_captions:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "media_localizations:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "media_customizations:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "media_stats:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "channels", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "channel:<channel_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "channel_episodes", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "channel_episodes_by_channel:<channel_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "tags", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "webinars", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "webinar:<webinar_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "stats_account", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "stats_account_by_date", PrimaryKey: "", IncKey: "date", IncStrategy: "merge"},
		{Name: "stats_events", PrimaryKey: "", IncKey: "received_at", IncStrategy: "merge"},
		{Name: "stats_events:<media_id>", PrimaryKey: "", IncKey: "received_at", IncStrategy: "merge"},
		{Name: "stats_events_by_visitor:<visitor_key>", PrimaryKey: "", IncKey: "received_at", IncStrategy: "merge"},
		{Name: "stats_visitors", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "stats_event:<event_key>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "stats_visitor:<visitor_key>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "stats_media:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "stats_media_by_date:<media_id>", PrimaryKey: "", IncKey: "date", IncStrategy: "merge"},
		{Name: "stats_media_engagement:<media_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "stats_project:<project_id>", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Zendesk - Customer service
	"zendesk": {
		{Name: "tickets", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "ticket_metrics", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "ticket_metric_events", PrimaryKey: "id", IncKey: "time", IncStrategy: "append"},
		{Name: "ticket_forms", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "users", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "groups", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "organizations", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "brands", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "sla_policies", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "activities", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "automations", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "targets", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "calls", PrimaryKey: "id", IncKey: "updated_at", IncStrategy: "merge"},
		{Name: "chats", PrimaryKey: "id", IncKey: "update_timestamp", IncStrategy: "merge"},
	},

	// Zoom - Video conferencing
	"zoom": {
		{Name: "meetings", PrimaryKey: "id", IncKey: "start_time", IncStrategy: "merge"},
		{Name: "users", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "participants", PrimaryKey: "id", IncKey: "join_time", IncStrategy: "merge"},
	},
}

// GetSourceTables returns the available tables for a specific ingestr source.
func GetSourceTables(sourceName string) (*Source, error) {
	tables, ok := SourceTablesRegistry[sourceName]
	if !ok {
		return nil, fmt.Errorf("source '%s' not found in registry", sourceName)
	}

	// Create a copy to avoid modifying the original registry
	sortedTables := make([]*SourceTable, len(tables))
	copy(sortedTables, tables)

	sort.Slice(sortedTables, func(i, j int) bool {
		return sortedTables[i].Name < sortedTables[j].Name
	})

	return &Source{
		Name:   sourceName,
		Tables: sortedTables,
	}, nil
}

// GetAllSources returns all available ingestr sources and their tables.
// Sources are sorted by name for consistent output.
func GetAllSources() []*Source {
	sources := make([]*Source, 0, len(SourceTablesRegistry))

	for name, tables := range SourceTablesRegistry {
		sources = append(sources, &Source{
			Name:   name,
			Tables: tables,
		})
	}

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Name < sources[j].Name
	})

	return sources
}
