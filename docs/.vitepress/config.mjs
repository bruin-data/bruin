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
                    {text: "Introduction", link: "/getting-started/introduction"},
                    {
                        text: "Concepts",
                        link: "/getting-started/concepts",
                        items: [
                            {text: "Design Principles", link: "/getting-started/concepts/design-principles"},
                            {text: "Glossary", link: "/getting-started/concepts/glossary"},
                        ]
                    }
                ],
            },
            {
                text: "Commands",
                collapsed: false,
                items: [
                    {text: "Init", link: "/commands/init"},
                    {text: "Connections", link: "/commands/connections"},
                    {text: "Run", link: "/commands/run"},
                ],
            },
            {
                text: "Connections",
                collapsed: false,
                items: [
                    {
                        text: "Overview", link: "/connections/overview", items: [
                            {text: "Adjust", link: "/connections/adjust.md"},
                            {text: "Airtable", link: "/connections/airtable.md"},
                            {text: "AWS Athena", link: "/connections/athena"},
                            {text: "Chess", link: "/connections/chess.md"},
                            {text: "Databricks", link: "/connections/databricks"},
                            {text: "DuckDB", link: "/connections/duckdb"},
                            {text: "Google Cloud Platform", link: "/connections/google_cloud_platform"},
                            {text: "Google Sheets", link: "/connections/google_sheets.md"},
                            {text: "Gorgias", link: "/connections/gorgias"},
                            {text: "Hubspot", link: "/connections/hubspot.md"},
                            {text: "Microsoft SQL Server", link: "/connections/mssql"},
                            {text: "MongoDB", link: "/connections/mongo"},
                            {text: "MySQL", link: "/connections/mysql"},
                            {text: "Notion", link: "/connections/notion"},
                            {text: "Postgres", link: "/connections/postgres"},
                            {text: "Redshift", link: "/connections/redshift"},
                            {text: "SAP HANA", link: "/connections/sap_hana"},
                            {text: "S3", link: "/connections/s3"},
                            {text: "Snowflake", link: "/connections/snowflake"},
                            {text: "Zendesk", link: "/connections/Zendesk"},
                        ]
                    },
                ],
            },
            {
                text: "Assets",
                collapsed: false,
                items: [
                    {text: "Definition Schema", link: "/assets/definition-schema", items:[
                        {text: "Google BigQuery", link: "/assets/bigquery"},
                        {text: "Snowflake", link: "/assets/snowflake"},
                        {text: "Python", link: "/assets/python"},
                        {text: "Ingestr", link: "/assets/ingestr"},
                        {text: "Databricks", link: "/assets/databricks"},
                        {text: "SQL Server", link: "/assets/mssql"},
                        {text: "Postgres", link: "/assets/postgres"},
                        {text: "Synapse", link: "/assets/synapse"},
                        {text: "Redshift", link: "/assets/redshift"},
                        {text: "AWS Athena", link: "/assets/athena"},
                    ]},
                    {text: "Materialization", link: "/assets/materialization"},
                ],
            },
            {
                text: "Templating",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/templating/templating"},
                    {text: "Filters", link: "/templating/filters"},
                ],
            },
            {
                text: "Quality checks",
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
                text: "Bruin Cloud",
                collapsed: false,
                items: [
                    {text: "Cross-pipeline dependencies", link: "/cloud/cross-pipeline"},
                    {text: "Notifications", link: "/cloud/notifications"},
                ],
            },
            {
                text:"Bruin VSCode Extension",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/vscode-extension/overview",
                    items: [
                        {text: "Installation", link: "/vscode-extension/installation"},
                        //{text: "Features", link: "/vscode-extension/features"},
                    ]
                },
                ]
            }
        ],

        socialLinks: [{icon: "github", link: "https://github.com/bruin-data/bruin"}],
    },
});
