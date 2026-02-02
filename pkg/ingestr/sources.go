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

	// Elasticsearch - Search and analytics engine (user-defined indices)
	"elasticsearch": {},

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

	// HubSpot - CRM platform
	"hubspot": {
		{Name: "companies", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "contacts", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "deals", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "tickets", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "products", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "quotes", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
		{Name: "schemas", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
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
		{Name: "tickets", PrimaryKey: "", IncKey: "updated_at", IncStrategy: "merge"},
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

	// Kafka - Event streaming (user-defined topics)
	"kafka": {},

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

	// SFTP (user-defined paths)
	"sftp": {},

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

	// TikTok Ads
	"tiktokads": {
		{Name: "custom:<dimensions>:<metrics>", PrimaryKey: "", IncKey: "", IncStrategy: "merge"},
	},

	// Trustpilot - Reviews platform
	"trustpilot": {
		{Name: "reviews", PrimaryKey: "", IncKey: "", IncStrategy: "replace"},
	},

	// Wise - Money transfers
	"wise": {
		{Name: "profiles", PrimaryKey: "id", IncKey: "", IncStrategy: "merge"},
		{Name: "transfers", PrimaryKey: "id", IncKey: "created", IncStrategy: "merge"},
		{Name: "balances", PrimaryKey: "id", IncKey: "modificationTime", IncStrategy: "merge"},
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
