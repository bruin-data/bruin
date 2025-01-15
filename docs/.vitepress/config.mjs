import {defineConfig} from "vitepress";
import jinja2Grammar from "shiki/langs/jinja-html.mjs";
import fs from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';

// Get the directory name of the current module
const __dirname = path.dirname(fileURLToPath(import.meta.url));


const bruinSqlGrammar = JSON.parse(
    fs.readFileSync(path.resolve(__dirname, "theme/bruinsql.json"), "utf8")
);

const bruinPythonGrammar = JSON.parse(
    fs.readFileSync(path.resolve(__dirname, "theme/bruinpython.json"), "utf8")
);

// https://vitepress.dev/reference/site-config
export default defineConfig({
    title: "Bruin CLI",
    description: "Open-source multi-language data pipelines",
    base: "/bruin/",
    head: [
        [
            "script",
            {
                async: "",
                src: "https://www.googletagmanager.com/gtag/js?id=G-VB8J5FETV5",
            },
        ],
        [
            "script",
            {},
            `window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());
            gtag('config', 'G-VB8J5FETV5');`,
        ],
    ],
    themeConfig: {
        outline: "deep",
        search: {
            provider: "local",
        },
        nav: [{text: "Home", link: "/"}],
        sidebar: [
            {
                collapsed: false,
                items: [
                    {
                        text: "Introduction",
                        link: "/",
                        items: [
                            {text: "Installation", link: "/getting-started/introduction/installation"},
                            {text: "Quickstart", link: "/getting-started/introduction/quickstart"},
                        ],
                    },
                    {
                        text: "Features",
                        link: "/getting-started/features",
                        items: [{text: "Glossary", link: "/getting-started/glossary"}]
                    },
                    {text: "Concepts", link: "/getting-started/concepts"},
                    {text: "Design Principles", link: "/getting-started/design-principles"},

                    {
                        text: "Templates",
                        link: "/getting-started/templates",
                        collapsed: true,
                        items: [
                            { text: "athena", link: "/getting-started/templates-docs/athena-README.md" },
                            { text: "chess", link: "/getting-started/templates-docs/chess-README.md" },
                            { text: "duckdb", link: "/getting-started/templates-docs/duckdb-README.md" },
                            { text: "firebase", link: "/getting-started/templates-docs/firebase-README.md" },
                            { text: "gorgias", link: "/getting-started/templates-docs/gorgias-README.md" },
                            { text: "gsheet-bigquery", link: "/getting-started/templates-docs/gsheet-bigquery-README.md" },
                            { text: "gsheet-duckdb", link: "/getting-started/templates-docs/gsheet-duckdb-README.md" },
                            { text: "notion", link: "/getting-started/templates-docs/notion-README.md" },
                            { text: "python", link: "/getting-started/templates-docs/python-README.md" },
                            { text: "shopify-bigquery", link: "/getting-started/templates-docs/shopify-bigquery-README.md" },
                            { text: "shopify-duckdb", link: "/getting-started/templates-docs/shopify-duckdb-README.md" },
                        ],
                    },

                    {
                           text: "Tutorials",
                           collapsed: true,
                           items: [
                              {text: "Your First Pipeline", link: "/getting-started/tutorials/first-tutorial"},
                              { text: "Load Notion to PostgreSQL", link: "/getting-started/tutorials/load-notion-postgres" },
                          ],

                     },
                    {
                        text: "VS Code Extension",
                        link: "/vscode-extension/overview",
                        collapsed: true,
                        items: [
                            {text: "Getting Started", link: "/vscode-extension/getting-started"},
                            {text: "Configuration", link: "/vscode-extension/configuration"},
                        ],
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
                text: "Assets",
                collapsed: false,
                items: [
                    {text: "Definition Schema", link: "/assets/definition-schema"},
                    {
                        text: "Asset Types", items: [
                            {text: "SQL", link: "/assets/sql"},
                            {text: "Seed", link: "/assets/seed"},
                            {text: "Ingestr", link: "/assets/ingestr"},
                            {text: "Python Assets", link: "/assets/python"},
                        ]
                    },
                    {text: "Columns", link: "/assets/columns"},
                    {text: "Materialization", link: "/assets/materialization"},
                    {
                        text: " Jinja Templating",
                        link: "/assets/templating/templating",
                        items: [{text: "Filters", link: "/assets/templating/filters"}],
                    },
                ],
            },
            {
                text: "Data Ingestion",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/ingestion/overview"},
                    {
                        text: "Sources",
                        collapsed: true,
                        items: [
                            {text: "Adjust", link: "/ingestion/adjust.md"},
                            {text: "Airtable", link: "/ingestion/airtable.md"},
                            {text: "AppStore", link: "/ingestion/appstore.md"},
                            {text: "Asana", link: "/ingestion/asana.md"},
                            {text: "Chess", link: "/ingestion/chess.md"},
                            {text: "DynamoDB", link: "/ingestion/dynamodb.md"},
                            {text: "Facebook", link: "/ingestion/facebook-ads.md"},
                            {text: "GitHub", link: "/ingestion/github.md"},
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
                            {text: "Shopify", link: "/ingestion/shopify"},
                            {text: "Stripe", link: "/ingestion/stripe.md"},
                            {text: "Slack", link: "/ingestion/slack.md"},
                            {text: "TikTok Ads", link: "/ingestion/tiktokads.md"},
                            {text: "Zendesk", link: "/ingestion/zendesk.md"},
                        ],
                    },
                    {text: "Destinations", link: "/ingestion/destinations.md"}
                ],
            },
            {
                text: "Quality Checks",
                collapsed: false,
                items: [
                    {
                        text: "Overview",
                        link: "/quality/overview",
                    },
                    {text: "Column Checks", link: "/quality/available_checks"},
                    {text: "Custom Checks", link: "/quality/custom"},
                ],
            },
            {
                text: "CI/CD Integration",
                collapsed: false,
                items: [{text: "Github Actions", link: "/cicd/github-action"}],
            },
            {
                text: "Commands",
                collapsed: false,
                items: [
                    {text: "Clean", link: "/commands/clean"},
                    {text: "Connections", link: "/commands/connections.md"},
                    {text: "Environments", link: "/commands/environments"},
                    {text: "Format", link: "/commands/format"},
                    {text: "Init", link: "/commands/init"},
                    {text: "Lineage", link: "/commands/lineage"},
                    {text: "Render", link: "/commands/render"},
                    {text: "Run", link: "/commands/run"},
                    {text: "Query", link: "/commands/query"},
                    {text: "Validate", link: "/commands/validate"},
                ],

            },
            {
                text: "Bruin Cloud",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/cloud/overview"},
                    {text: "Cross-pipeline dependencies", link: "/cloud/cross-pipeline"},
                    {text: "dbt Projects", link: "/cloud/dbt"},
                    {text: "Notifications", link: "/cloud/notifications"},
                    {text: "Instance Types", link: "/cloud/instance-types"},
                ],
            },
        ],

        socialLinks: [{icon: "github", link: "https://github.com/bruin-data/bruin"}],
    },

    markdown: {
        languages: ["sql", "yaml", "shell", "python", "json", jinja2Grammar, bruinSqlGrammar, bruinPythonGrammar],
    },
});
