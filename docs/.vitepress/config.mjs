import {defineConfig} from "vitepress";
import jinja2Grammar from "shiki/langs/jinja-html.mjs";
import fs from 'fs';
import path from 'path';
import {fileURLToPath} from 'url';


import { withMermaid } from "vitepress-plugin-mermaid";

// Get the directory name of the current module
const __dirname = path.dirname(fileURLToPath(import.meta.url));


const bruinSqlGrammar = JSON.parse(
    fs.readFileSync(path.resolve(__dirname, "theme/bruinsql.json"), "utf8")
);

const bruinPythonGrammar = JSON.parse(
    fs.readFileSync(path.resolve(__dirname, "theme/bruinpython.json"), "utf8")
);

// https://vitepress.dev/reference/site-config
export default withMermaid({
    title: "Bruin CLI",
    description: "Open-source multi-language data pipelines",
    base: "/bruin/",
    sitemap: {
        hostname: 'https://getbruin.com',
        transformItems: (items) => {
          return items.map((item) => {
            const cleaned = item.url.replace(/^\/bruin\//, '');
            item.url = `https://getbruin.com/docs/bruin/${cleaned}`;
            return item;
          });
        },
        trailingSlash: true,
      },
    head: [
        [
            "script",
            {},
            `(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
})(window,document,'script','dataLayer','GTM-K2L7S5FP');`,
        ],
        [
            "noscript",
            {},
            `<iframe src="https://www.googletagmanager.com/ns.html?id=GTM-K2L7S5FP" height="0" width="0" style="display:none;visibility:hidden"></iframe>`,
        ],
    ],
    transformPageData(pageData) {
        // Build the canonical URL for each page
        const canonicalUrl = `https://getbruin.com/docs/bruin/${pageData.relativePath}`
          .replace(/index\.md$/, '')
          .replace(/\.md$/, '.html');
        
        // Add canonical link to every page's head
        pageData.frontmatter.head ??= [];
        pageData.frontmatter.head.push([
          'link',
          { rel: 'canonical', href: canonicalUrl }
        ]);
    },
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
                        items: [
                            {text: "Pipeline", link: "/getting-started/pipeline"},
                            {text: "Glossary", link: "/getting-started/glossary"},
                            {text: "Policies", link: "/getting-started/policies"},
                            {text: "Environments", link: "/getting-started/devenv"},
                            {text: "Variables", link: "/getting-started/pipeline-variables"},
                            {text: "Bruin MCP", link: "/getting-started/bruin-mcp"},
                        ]
                    },
                    {text: "Concepts", link: "/getting-started/concepts"},
                    {text: "Design Principles", link: "/getting-started/design-principles"},

                    {
                        text: "Templates",
                        link: "/getting-started/templates",
                        collapsed: true,
                        items: [
                            { text: "athena", link: "/getting-started/templates-docs/athena-README" },
                            { text: "clickhouse", link: "/getting-started/templates-docs/clickhouse-README" },
                            { text: "chess", link: "/getting-started/templates-docs/chess-README" },
                            { text: "duckdb", link: "/getting-started/templates-docs/duckdb-README" },
                            { text: "firebase", link: "/getting-started/templates-docs/firebase-README" },
                            { text: "frankfurter", link: "/getting-started/templates-docs/frankfurter-README" },
                            { text: "gorgias", link: "/getting-started/templates-docs/gorgias-README" },
                            { text: "gsheet-bigquery", link: "/getting-started/templates-docs/gsheet-bigquery-README" },
                            { text: "gsheet-duckdb", link: "/getting-started/templates-docs/gsheet-duckdb-README" },
                            { text: "notion", link: "/getting-started/templates-docs/notion-README" },
                            { text: "python", link: "/getting-started/templates-docs/python-README" },
                            { text: "shopify-bigquery", link: "/getting-started/templates-docs/shopify-bigquery-README" },
                            { text: "shopify-duckdb", link: "/getting-started/templates-docs/shopify-duckdb-README" },
                        ],
                    },

                    {
                           text: "Tutorials",
                           collapsed: true,
                           items: [
                          {text: "Your First Pipeline", link: "/getting-started/tutorials/first-tutorial"},
                              { text: "Load Notion to PostgreSQL", link: "/getting-started/tutorials/load-notion-postgres" },
                               { text: "Using Templates", link: "/getting-started/tutorials/template-tutorial" },
                               { text: "Oracle to DuckDB", link: "/getting-started/tutorials/oracle-duckdb" },
                          ],

                     },
                    {
                        text: "VS Code Extension",
                        link: "/vscode-extension/overview",
                        collapsed: true,
                        items: [
                            {text: "Getting Started", link: "/vscode-extension/getting-started"},
                            {text: "Configuration", link: "/vscode-extension/configuration"},
                            {text: "Panels Overview", link: "/vscode-extension/panel-overview",
                                items: [
                                    {text: "Side Panel", 
                                        link: "/vscode-extension/panels/side-panel/side-panel",
                                        items: [
                                            {text: "Tabs", link: "/vscode-extension/panels/side-panel/tabs"},
                                            {text: "Editor Experience", link: "/vscode-extension/panels/side-panel/editor-experience"},
                                        ]},
                                    {text: "Lineage", link: "/vscode-extension/panels/lineage-panel"},
                                    {text: "Query Preview", link: "/vscode-extension/panels/query-preview"},
                                ],
                            },
                        ],
                    },

                ],
            },
            {
                text: "Data Platforms",
                collapsed: false,
                items: [
                    {text: "AWS Athena", link: "/platforms/athena"},
                    {text: "Clickhouse", link: "/platforms/clickhouse"},
                    {text: "Databricks", link: "/platforms/databricks"},
                    {text: "DuckDB", link: "/platforms/duckdb"},
                    {text: "MotherDuck", link: "/platforms/motherduck"},
                    {text: "Oracle", link: "/platforms/oracle"},
                    {text: "Google BigQuery", link: "/platforms/bigquery"},
                    {text: "Microsoft SQL Server", link: "/platforms/mssql"},
                    {text: "MySQL", link: "/platforms/mysql"},
                    {text: "Postgres", link: "/platforms/postgres"},
                    {text: "Redshift", link: "/platforms/redshift"},
                    {text: "Snowflake", link: "/platforms/snowflake"},
                    {text: "Synapse", link: "/platforms/synapse"},
                    {text: "S3", link: "/platforms/s3"},
                    {text: "Trino", link: "/platforms/trino"},

                    {text: "AWS EMR Serverless", link: "/platforms/emr_serverless"},
                    {text: "GCP Dataproc Serverless", link: "/platforms/dataproc_serverless"},
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
                            {text: "Python", link: "/assets/python"},
                            {text: "R", link: "/assets/r"},
                            {text: "Sensor", link: "/assets/sensor"},
                            {text: "Dashboard", link: "/assets/dashboard", items: [
                                {text: "Tableau", link: "/assets/tableau-refresh"},
                            ]},
                        ]
                    },
                    {text: "Columns", link: "/assets/columns"},
                    {text: "Credentials", link: "/getting-started/credentials"},
                    {text: "Interval Modifiers", link: "/assets/interval-modifiers"},
                    {text: "Materialization", link: "/assets/materialization"},
                    {
                        text: " Jinja Templating",
                        link: "/assets/templating/templating",
                        items: [
                            {text: "Filters", link: "/assets/templating/filters"},
                            {text: "Macros", link: "/assets/templating/macros"}
                        ],
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
                            {text: "Adjust", link: "/ingestion/adjust"},
                            {text: "Airtable", link: "/ingestion/airtable"},
                            {text: "Anthropic", link: "/ingestion/anthropic"},
                            {text: "Allium", link: "/ingestion/allium"},
                            {text: "AppStore", link: "/ingestion/appstore"},
                            {text: "AppLovin", link: "/ingestion/applovin"},
                            {text: "AppLovin Max", link: "/ingestion/applovin_max"},
                            {text: "Asana", link: "/ingestion/asana"},
                            {text: "Attio", link: "/ingestion/attio"},
                            {text: "Bruin", link: "/ingestion/bruin"},
                            {text: "Chess", link: "/ingestion/chess"},
                            {text: "ClickUp", link: "/ingestion/clickup"},
                            {text: "Couchbase", link: "/ingestion/couchbase"},
                            {text: "Cursor", link: "/ingestion/cursor"},
                            {text: "DB2", link: "/ingestion/db2"},
                            {text: "Docebo", link: "/ingestion/docebo"},
                            {text: "DynamoDB", link: "/ingestion/dynamodb"},
                            {text: "Elasticsearch", link: "/ingestion/elasticsearch"},
                            {text: "Facebook", link: "/ingestion/facebook-ads"},
                            {text: "Fireflies", link: "/ingestion/fireflies"},
                            {text: "Fluxx", link: "/ingestion/fluxx"},
                            {text: "Frankfurter", link: "/ingestion/frankfurter"},
                            {text: "FundraiseUp", link: "/ingestion/fundraiseup"},
                            {text: "Freshdesk", link: "/ingestion/freshdesk"},
                            {text: "GCP Spanner", link: "/ingestion/spanner"},
                            {text: "GitHub", link: "/ingestion/github"},
                            {text: "Google Cloud Storage", link: "/ingestion/gcs"},
                            {text: "Google Sheets", link: "/ingestion/google_sheets"},
                            {text: "Google Ads", link: "/ingestion/google-ads"},
                            {text: "Google Analytics", link: "/ingestion/google_analytics"},
                            {text: "Gorgias", link: "/ingestion/gorgias"},
                            {text: "Hostaway", link: "/ingestion/hostaway"},
                            {text: "Hubspot", link: "/ingestion/hubspot"},
                            {text: "Indeed", link: "/ingestion/indeed"},
                            {text: "Intercom", link: "/ingestion/intercom"},
                            {text: "InfluxDB", link: "/ingestion/influxdb"},
                            {text: "Internet Society Pulse", link: "/ingestion/isoc_pulse"},
                            {text: "Jira", link: "/ingestion/jira"},
                            {text: "Kafka", link: "/ingestion/kafka"},
                            {text: "Kinesis", link: "/ingestion/kinesis"},
                            {text: "Klaviyo", link: "/ingestion/klaviyo"},
                            {text: "Monday", link: "/ingestion/monday"},
                            {text: "Plus Vibe AI", link: "/ingestion/plusvibeai"},
                            {text: "LinkedIn Ads", link: "/ingestion/linkedinads"},
                            {text: "Mailchimp", link: "/ingestion/mailchimp"},
                            {text: "Linear", link: "/ingestion/linear"},
                            {text: "Mixpanel", link: "/ingestion/mixpanel"},
                            {text: "MongoDB", link: "/ingestion/mongo"},
                            {text: "MySQL", link: "/ingestion/mysql"},
                            {text: "Notion", link: "/ingestion/notion"},
                            {text: "Personio", link: "/ingestion/personio"},
                            {text: "PhantomBuster", link: "/ingestion/phantombuster"},
                            {text: "Pipedrive", link: "/ingestion/pipedrive"},
                            {text: "Pinterest", link: "/ingestion/pinterest"},
                            {text: "Primer", link: "/ingestion/primer"},
                            {text: "Trustpilot", link: "/ingestion/trustpilot"},
                            {text: "QuickBooks", link: "/ingestion/quickbooks"},
                            {text: "Revenuecat", link: "/ingestion/revenuecat"},
                            {text: "Salesforce", link: "/ingestion/salesforce"},
                            {text: "SAP HANA", link: "/ingestion/sap_hana"},
                            {text: "S3", link: "/ingestion/s3"},
                            {text: "SFTP", link: "/ingestion/sftp"},
                            {text: "Shopify", link: "/ingestion/shopify"},
                            {text: "Smartsheet", link: "/ingestion/smartsheet"},
                            {text: "Snapchat Ads", link: "/ingestion/snapchat-ads"},
                            {text: "Solidgate", link: "/ingestion/solidgate"},
                            {text: "Stripe", link: "/ingestion/stripe"},
                            {text: "Slack", link: "/ingestion/slack"},
                            {text: "Socrata", link: "/ingestion/socrata"},
                            {text: "SQLite", link: "/ingestion/sqlite"},
                            {text: "TikTok Ads", link: "/ingestion/tiktokads"},
                            {text: "Wise", link: "/ingestion/wise"},
                            {text: "Zendesk", link: "/ingestion/zendesk"},
                            {text: "Zoom", link: "/ingestion/zoom"},
                            
                        ],
                    },
                    {text: "Destinations", link: "/ingestion/destinations"}
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
                text: "Secret Providers",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/secrets/overview"},
                    {text: "Hashicorp Vault", link: "/secrets/vault"}
                ]
            },
            {
                text: "Deployment",
                collapsed: false,
                items: [
                    {text: "Ubuntu VM with Cron", link: "/deployment/vm-deployment"},
                    {text: "Apache Airflow", link: "/deployment/airflow"},
                    {text: "GitHub Actions", link: "/deployment/github-actions"},
                    {text: "GitLab CI/CD", link: "/deployment/cloud/gitlab-cicd"},
                    {
                        text: "AWS",
                        collapsed: true,
                        items: [
                            {text: "Lambda", link: "/deployment/cloud/aws-lambda"},
                            {text: "ECS", link: "/deployment/cloud/aws-ecs"}
                        ]
                    },
                    {
                        text: "Google Cloud",
                        collapsed: true,
                        items: [
                            {text: "Cloud Run", link: "/deployment/cloud/google-cloud-run"}
                        ]
                    }
                ],
            },
            {
                text: "CI/CD Integration",
                collapsed: false,
                items: [
                    {text: "GitHub Actions", link: "/cicd/github-action"},
                    {text: "GitLab CI/CD", link: "/cicd/gitlab-ci"},
                    {text: "CircleCI", link: "/cicd/circleci"},
                    {text: "Jenkins", link: "/cicd/jenkins"},
                    {text: "Azure Pipelines", link: "/cicd/azure-pipelines"},
                ],
            },
            {
                text: "Commands",
                collapsed: false,
                items: [
                    {text: "AI Enhance", link: "/commands/ai-enhance"},
                    {text: "Init", link: "/commands/init"},
                    {text: "Clean", link: "/commands/clean"},
                    {text: "Connections", link: "/commands/connections"},
                    {text: "Data Diff", link: "/commands/data-diff"},
                    {text: "Environments", link: "/commands/environments"},
                    {text: "Format", link: "/commands/format"},
                    {text: "Import", link: "/commands/import"},
                    {text: "Lineage", link: "/commands/lineage"},
                    {text: "Patch", link: "/commands/patch"},
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
                    {text: "Developer Environments", link: "/cloud/developer-environments"},
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

    mermaid: {
        // refer https://mermaid.js.org/config/setup/modules/mermaidAPI.html#mermaidapi-configuration-defaults for options
    },
    // optionally set additional config for plugin itself with MermaidPluginConfig
    mermaidPlugin: {
        class: "mermaid my-class", // set additional css classes for parent container
    },
});
