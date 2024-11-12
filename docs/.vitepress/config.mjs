import {defineConfig} from "vitepress";

// https://vitepress.dev/reference/site-config
export default defineConfig({
    title: "Bruin CLI",
    description: "Open-source multi-language data pipelines",
    base: '/bruin/',
    themeConfig: {
        outline: 'deep',
        search: {
            provider: 'local'
        },
        nav: [{text: "Home", link: "/"}],
        sidebar: [
            {
                text: "Getting Started",
                collapsed: false,
                items: [
                    {
                        text: "Introduction", link: "/", items: [
                            {text: "Installation", link: "/getting-started/introduction/installation"},
                            {text: "Quickstart", link: "/getting-started/introduction/quickstart"},
                        ]
                    },
                    {text: "Concepts", link: "/getting-started/concepts"},
                    {text: "Design Principles", link: "/getting-started/design-principles"},
                    {text: "Glossary", link: "/getting-started/glossary"},
                    {text: "Tutorials", link: "/getting-started/tutorials"},
                ],
            },
            {
                text: "Assets",
                collapsed: false,
                items: [
                    {text: "Definition Schema", link: "/assets/definition-schema"},
                    {text: "Columns", link: "/assets/columns"},
                    {text: "Materialization", link: "/assets/materialization"},
                    {text: "Ingestr", link: "/assets/ingestr"},
                    {text: "Python Assets", link: "/assets/python"},
                    {
                        text: "Templating", link: "/templating/templating", items: [
                            {text: "Filters", link: "/templating/filters"},

                        ]
                    },
                ],
            },
            {
                text: "Data Platforms",
                collapsed: false,
                items: [
                    {text: "AWS Athena", link: "/platforms/athena"},
                    {text: "Databricks", link: "/platforms/databricks"},
                    {text: "DuckDB", link: "/platforms/duckdb"},
                    {text: "Google BigQuery", link: "/platforms/bigquery"},
                    {text: "Microsoft SQL Server", link: "/platforms/mssql"},
                    {text: "Postgres", link: "/platforms/postgres"},
                    {text: "Redshift", link: "/platforms/redshift"},
                    {text: "Snowflake", link: "/platforms/snowflake"},
                    {text: "Synapse", link: "/platforms/synapse"},
                ],
            },
            {
                text: "Data Ingestion",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/ingestion/overview"},
                    {
                        text: "Sources", collapsed: true, items: [
                            {text: "Adjust", link: "/ingestion/adjust.md"},
                            {text: "Airtable", link: "/ingestion/airtable.md"},
                            {text: "Chess", link: "/ingestion/chess.md"},
                            {text: "Facebook", link: "/ingestion/facebook-ads.md"},
                            {text: "Google Sheets", link: "/ingestion/google_sheets.md"},
                            {text: "Gorgias", link: "/ingestion/gorgias"},
                            {text: "Hubspot", link: "/ingestion/hubspot.md"},
                            {text: "Kafka", link: "/ingestion/kafka.md"},
                            {text: "Klaviyo", link: "/ingestion/klaviyo.md"},
                            {text: "MongoDB", link: "/ingestion/mongo"},
                            {text: "MySQL", link: "/ingestion/mysql"},
                            {text: "Notion", link: "/ingestion/notion"},
                            {text: "SAP HANA", link: "/ingestion/sap_hana"},
                            {text: "S3", link: "/ingestion/s3"},
                            {text: "Slack", link: "/ingestion/slack.md"},
                            {text: "Zendesk", link: "/ingestion/zendesk.md"},
                        ]
                    },
                ],
            },
            {
                text: "Commands",
                collapsed: false,
                items: [
                    {text: "Init", link: "/commands/init"},
                    {text: "Connections", link: "/commands/connections.md"},
                    {text: "Run", link: "/commands/run"},
                ],
            },
            {
                text: "Quality Checks",
                collapsed: false,
                items: [
                    {
                        text: "Overview", link: "/quality/overview", items: [
                            {text: "Not null", link: "/quality/not_null"},
                            {text: "Unique", link: "/quality/unique"},
                            {text: "Positive", link: "/quality/positive"},
                            {text: "Non Negative", link: "/quality/non_negative"},
                            {text: "Negative", link: "/quality/negative"},
                            {text: "Accepted values", link: "/quality/accepted_values"},
                            {text: "Pattern", link: "/quality/pattern"},
                        ]
                    },
                ],
            },
            {
                text: "Bruin VSCode Extension",
                collapsed: false,
                items: [
                    {
                        text: "Overview", link: "/vscode-extension/overview",
                        items: [
                            {text: "Installation", link: "/vscode-extension/installation"},
                            {text: "Getting Started", link: "/vscode-extension/getting-started"},
                            {text: "Configuration", link: "/vscode-extension/configuration"},
                            {
                                text: "Panels",
                                items: [
                                    {text: "Side Panel", link: "/vscode-extension/panels/side-panel"},
                                    {text: "Lineage Panel", link: "/vscode-extension/panels/lineage-panel"}
                                ]
                            },
                        ]
                    },
                ]
            },
            {
                text: "CI/CD Integration",
                collapsed: false,
                items: [
                    {text: "Github Actions", link: "/cicd/github-action"},
                ],
            },
            {
                text: "Bruin Cloud",
                collapsed: false,
                items: [
                    {text: "Cross-pipeline dependencies", link: "/cloud/cross-pipeline"},
                    {text: "Notifications", link: "/cloud/notifications"},
                ],
            },

        ],

        socialLinks: [{icon: "github", link: "https://github.com/bruin-data/bruin"}],
    },
});
