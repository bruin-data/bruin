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
                        text: "Core Concepts",
                        link: "/core-concepts/overview",
                        items: [
                            {text: "Asset", link: "/assets/definition-schema"},
                            {text: "Pipeline", link: "/pipelines/definition"},
                            {text: "Commands", link: "/commands/overview"},
                            {text: "Project", link: "/core-concepts/project"},
                            {text: "Variables", link: "/variables/overview"},
                            {text: "Semantic Layer", link: "/core-concepts/semantic-layer"},
                        ],
                    },
                    {text: "Design Principles", link: "/getting-started/design-principles"},
                    {
                        text: "Learning Material",
                        collapsed: false,
                        items: [
                            {text: "Bruin Academy", link: "https://getbruin.com/learn/"},
                            {text: "Your First Pipeline", link: "/getting-started/tutorials/first-tutorial"},
                            {text: "Explore Example Project", link: "/getting-started/tutorials/example-project"},
                        ],
                    },
                ],
            },
            {
                text: "Bruin Cloud",
                collapsed: true,
                items: [
                    {text: "Overview", link: "/cloud/overview"},
                    {text: "Getting Started", link: "/cloud/getting-started"},
                    {text: "Projects", link: "/cloud/projects"},
                    {text: "Connections", link: "/cloud/connections"},
                    {text: "Pipelines", link: "/cloud/pipelines"},
                    {text: "Runs", link: "/cloud/runs"},
                    {text: "Backfills", link: "/cloud/backfills"},
                    {text: "Assets", link: "/cloud/assets"},
                    {text: "Catalog", link: "/cloud/catalog"},
                    {text: "Insights", link: "/cloud/insights"},
                    {text: "Dashboards", link: "/cloud/dashboards"},
                    {
                        text: "AI Agents",
                        collapsed: true,
                        items: [
                            {text: "Overview", link: "/cloud/ai-agents/overview"},
                            {text: "Configure Agents", link: "/cloud/ai-agents/configure"},
                            {text: "Chat with Agents", link: "/cloud/ai-agents/chat"},
                            {text: "Scheduled Agents", link: "/cloud/ai-agents/scheduled"},
                            {text: "Databricks OAuth", link: "/cloud/ai-agents/databricks-oauth"},
                            {
                                text: "Examples",
                                collapsed: true,
                                items: [
                                    {text: "Slack AI Analyst Tutorial", link: "/cloud/ai-agents/slack-ai-analyst"},
                                ],
                            },
                        ],
                    },
                    {
                        text: "Integrations",
                        collapsed: true,
                        items: [
                            {text: "Overview", link: "/cloud/integrations/overview"},
                            {text: "Slack", link: "/cloud/integrations/slack"},
                            {text: "Microsoft Teams", link: "/cloud/integrations/teams"},
                            {text: "Google Chat", link: "/cloud/integrations/google-chat"},
                            {text: "Discord", link: "/cloud/integrations/discord"},
                            {text: "WhatsApp", link: "/cloud/integrations/whatsapp"},
                            {text: "Telegram", link: "/cloud/integrations/telegram"},
                        ],
                    },
                    {text: "Notifications", link: "/cloud/notifications"},
                    {text: "Cross-pipeline Dependencies", link: "/cloud/cross-pipeline"},
                    {text: "Governance", link: "/cloud/governance"},
                    {text: "Instance Types", link: "/cloud/instance-types"},
                    {
                        text: "Security",
                        link: "/cloud/security",
                        collapsed: true,
                        items: [
                            {text: "Bruin's IP Addresses", link: "/cloud/security/ip-addresses"},
                        ],
                    },
                    {
                        text: "Team",
                        collapsed: true,
                        items: [
                            {text: "Team Settings", link: "/cloud/team-settings"},
                            {text: "API Tokens", link: "/cloud/api-tokens"},
                            {text: "Audit Logs", link: "/cloud/audit-logs"},
                        ],
                    },
                    {text: "Cloud MCP", link: "/cloud/mcp-setup"},
                    {text: "FAQ", link: "/cloud/faq"},
                ],
            },
            {
                text: "Pipelines",
                link: "/pipelines/definition",
                collapsed: false,
                items: [
                    {text: "Definition", link: "/pipelines/definition"},
                    {text: "Scheduling", link: "/pipelines/definition#schedule"},
                    {text: "Default Connections", link: "/pipelines/definition#default-connections"},
                    {text: "Pipeline Defaults", link: "/pipelines/definition#default-pipeline-level-defaults"},
                    {text: "Variants", link: "/pipelines/variants"},
                    {text: "Concurrency", link: "/getting-started/concurrency"},
                ],
            },
            {
                text: "Assets",
                link: "/assets/definition-schema",
                collapsed: false,
                items: [
                    {text: "Definition Schema", link: "/assets/definition-schema"},
                    {
                        text: "Asset Types", items: [
                            {text: "SQL", link: "/assets/sql"},
                            {text: "Seed", link: "/assets/seed"},
                            {text: "Ingestr", link: "/assets/ingestr"},
                            {text: "Python", link: "/assets/python", items: [
                                {text: "Python SDK", link: "/assets/python-sdk"},
                            ]},
                            {text: "R", link: "/assets/r"},
                            {text: "Sensor", link: "/assets/sensor"},
                            {text: "Dashboard", link: "/assets/dashboard", items: [
                                {text: "Tableau", link: "/assets/tableau-refresh"},
                            ]},
                        ]
                    },
                    {text: "Columns", link: "/assets/columns"},
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
                text: "Variables",
                link: "/variables/overview",
                collapsed: false,
                items: [
                    {text: "Built-in Variables", link: "/variables/built-in"},
                    {text: "Custom Variables", link: "/variables/custom"},
                ],
            },
            {
                text: "Connections & Platforms",
                link: "/connections/overview",
                collapsed: false,
                items: [
                    {
                        text: "Warehouses & Lakes",
                        collapsed: false,
                        items: [
                            {text: "Lakehouse Support", link: "/getting-started/lakehouse"},
                            {text: "AWS Athena", link: "/platforms/athena"},
                            {text: "Clickhouse", link: "/platforms/clickhouse"},
                            {text: "Databricks", link: "/platforms/databricks"},
                            {text: "Apache Doris", link: "/platforms/doris"},
                            {text: "DuckDB", link: "/platforms/duckdb"},
                            {text: "MotherDuck", link: "/platforms/motherduck"},
                            {text: "Oracle", link: "/platforms/oracle"},
                            {text: "Google BigQuery", link: "/platforms/bigquery"},
                            {text: "Microsoft SQL Server", link: "/platforms/mssql"},
                            {text: "Microsoft Fabric", link: "/platforms/fabric"},
                            {text: "MySQL", link: "/platforms/mysql"},
                            {text: "Postgres", link: "/platforms/postgres"},
                            {text: "Redshift", link: "/platforms/redshift"},
                            {text: "Snowflake", link: "/platforms/snowflake"},
                            {text: "StarRocks", link: "/platforms/starrocks"},
                            {text: "Synapse", link: "/platforms/synapse"},
                            {text: "S3", link: "/platforms/s3"},
                            {text: "Trino", link: "/platforms/trino"},
                            {text: "Dremio", link: "/platforms/dremio"},
                            {text: "Sail", link: "/platforms/sail"},
                            {text: "AWS EMR Serverless", link: "/platforms/emr_serverless"},
                            {text: "GCP Dataproc Serverless", link: "/platforms/dataproc_serverless"},
                        ],
                    },
                    {
                        text: "Data Ingestion",
                        collapsed: false,
                        items: [
                            {text: "Overview", link: "/ingestion/overview"},
                            {
                                text: "Sources",
                                collapsed: false,
                                items: [
                                    {text: "Adjust", link: "/ingestion/adjust"},
                                    {text: "Airtable", link: "/ingestion/airtable"},
                                    {text: "Amplitude", link: "/ingestion/amplitude"},
                                    {text: "Anthropic", link: "/ingestion/anthropic"},
                                    {text: "Allium", link: "/ingestion/allium"},
                                    {text: "API-Football", link: "/ingestion/api-football"},
                                    {text: "Apple Ads", link: "/ingestion/apple-ads"},
                                    {text: "AppStore", link: "/ingestion/appstore"},
                                    {text: "AppLovin", link: "/ingestion/applovin"},
                                    {text: "AppLovin Max", link: "/ingestion/applovin_max"},
                                    {text: "Asana", link: "/ingestion/asana"},
                                    {text: "Attio", link: "/ingestion/attio"},
                                    {text: "BallDontLie FIFA", link: "/ingestion/balldontlie"},
                                    {text: "Braze", link: "/ingestion/braze"},
                                    {text: "Bruin", link: "/ingestion/bruin"},
                                    {text: "Chargebee", link: "/ingestion/chargebee"},
                                    {text: "Chess", link: "/ingestion/chess"},
                                    {text: "ClickUp", link: "/ingestion/clickup"},
                                    {text: "Couchbase", link: "/ingestion/couchbase"},
                                    {text: "Cursor", link: "/ingestion/cursor"},
                                    {text: "Customer.io", link: "/ingestion/customerio"},
                                    {text: "DB2", link: "/ingestion/db2"},
                                    {text: "Docebo", link: "/ingestion/docebo"},
                                    {text: "Dune", link: "/ingestion/dune"},
                                    {text: "DynamoDB", link: "/ingestion/dynamodb"},
                                    {text: "Elasticsearch", link: "/ingestion/elasticsearch"},
                                    {text: "ESPN", link: "/ingestion/espn"},
                                    {text: "Facebook", link: "/ingestion/facebook-ads"},
                                    {text: "FastSpring", link: "/ingestion/fastspring"},
                                    {text: "Fireflies", link: "/ingestion/fireflies"},
                                    {text: "Fluxx", link: "/ingestion/fluxx"},
                                    {text: "football-data.org", link: "/ingestion/footballdata"},
                                    {text: "Frankfurter", link: "/ingestion/frankfurter"},
                                    {text: "FundraiseUp", link: "/ingestion/fundraiseup"},
                                    {text: "Freshdesk", link: "/ingestion/freshdesk"},
                                    {text: "GCP Spanner", link: "/ingestion/spanner"},
                                    {text: "GitHub", link: "/ingestion/github"},
                                    {text: "GitLab", link: "/ingestion/gitlab"},
                                    {text: "Google Cloud Storage", link: "/ingestion/gcs"},
                                    {text: "Google Sheets", link: "/ingestion/google_sheets"},
                                    {text: "Google Ads", link: "/ingestion/google-ads"},
                                    {text: "Google Analytics", link: "/ingestion/google_analytics"},
                                    {text: "Google Search Console", link: "/ingestion/gsc"},
                                    {text: "Gorgias", link: "/ingestion/gorgias"},
                                    {text: "G2", link: "/ingestion/g2"},
                                    {text: "Hostaway", link: "/ingestion/hostaway"},
                                    {text: "Hubspot", link: "/ingestion/hubspot"},
                                    {text: "Indeed", link: "/ingestion/indeed"},
                                    {text: "Intercom", link: "/ingestion/intercom"},
                                    {text: "InfluxDB", link: "/ingestion/influxdb"},
                                    {text: "Internet Society Pulse", link: "/ingestion/isoc_pulse"},
                                    {text: "Jira", link: "/ingestion/jira"},
                                    {text: "JobTread", link: "/ingestion/jobtread"},
                                    {text: "Kafka", link: "/ingestion/kafka"},
                                    {text: "Kinesis", link: "/ingestion/kinesis"},
                                    {text: "Klaviyo", link: "/ingestion/klaviyo"},
                                    {text: "Monday", link: "/ingestion/monday"},
                                    {text: "Plus Vibe AI", link: "/ingestion/plusvibeai"},
                                    {text: "LinkedIn Ads", link: "/ingestion/linkedinads"},
                                    {text: "Mailchimp", link: "/ingestion/mailchimp"},
                                    {text: "Linear", link: "/ingestion/linear"},
                                    {text: "Mixpanel", link: "/ingestion/mixpanel"},
                                    {text: "Microsoft OneLake", link: "/ingestion/onelake"},
                                    {text: "MongoDB", link: "/ingestion/mongo"},
                                    {text: "MySQL", link: "/ingestion/mysql"},
                                    {text: "Notion", link: "/ingestion/notion"},
                                    {text: "Paddle", link: "/ingestion/paddle"},
                                    {text: "Personio", link: "/ingestion/personio"},
                                    {text: "PhantomBuster", link: "/ingestion/phantombuster"},
                                    {text: "Pipedrive", link: "/ingestion/pipedrive"},
                                    {text: "Pinterest", link: "/ingestion/pinterest"},
                                    {text: "PlanetScale", link: "/ingestion/planetscale"},
                                    {text: "PostHog", link: "/ingestion/posthog"},
                                    {text: "Primer", link: "/ingestion/primer"},
                                    {text: "Trustpilot", link: "/ingestion/trustpilot"},
                                    {text: "QuickBooks", link: "/ingestion/quickbooks"},
                                    {text: "RabbitMQ", link: "/ingestion/rabbitmq"},
                                    {text: "Recurly", link: "/ingestion/recurly"},
                                    {text: "Reddit Ads", link: "/ingestion/reddit_ads"},
                                    {text: "Revenuecat", link: "/ingestion/revenuecat"},
                                    {text: "Salesforce", link: "/ingestion/salesforce"},
                                    {text: "SAP HANA", link: "/ingestion/sap_hana"},
                                    {text: "S3", link: "/ingestion/s3"},
                                    {text: "SendGrid", link: "/ingestion/sendgrid"},
                                    {text: "SFTP", link: "/ingestion/sftp"},
                                    {text: "SharePoint", link: "/ingestion/sharepoint"},
                                    {text: "Shopify", link: "/ingestion/shopify"},
                                    {text: "Smartsheet", link: "/ingestion/smartsheet"},
                                    {text: "Snapchat Ads", link: "/ingestion/snapchat-ads"},
                                    {text: "Solidgate", link: "/ingestion/solidgate"},
                                    {text: "Square", link: "/ingestion/square"},
                                    {text: "StarRocks", link: "/ingestion/starrocks"},
                                    {text: "Stripe", link: "/ingestion/stripe"},
                                    {text: "Slack", link: "/ingestion/slack"},
                                    {text: "Socrata", link: "/ingestion/socrata"},
                                    {text: "SQLite", link: "/ingestion/sqlite"},
                                    {text: "SurveyMonkey", link: "/ingestion/surveymonkey"},
                                    {text: "TikTok Ads", link: "/ingestion/tiktokads"},
                                    {text: "Trello", link: "/ingestion/trello"},
                                    {text: "Twilio", link: "/ingestion/twilio"},
                                    {text: "Vitess", link: "/ingestion/vitess"},
                                    {text: "Wise", link: "/ingestion/wise"},
                                    {text: "Zendesk", link: "/ingestion/zendesk"},
                                    {text: "Zoom", link: "/ingestion/zoom"},
                                ],
                            },
                        ],
                    },
                ],
            },
            {
                text: "Commands",
                link: "/commands/overview",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/commands/overview"},
                    {text: "Run", link: "/commands/run"},
                    {text: "Validate", link: "/commands/validate"},
                    {text: "Unit Test", link: "/commands/unit-test"},
                    {text: "Init", link: "/commands/init"},
                    {text: "AI Skills", link: "/commands/ai-skills"},
                    {text: "Clean", link: "/commands/clean"},
                    {text: "Connections", link: "/commands/connections"},
                    {text: "Data Diff", link: "/commands/data-diff"},
                    {text: "Environments", link: "/commands/environments"},
                    {text: "Format", link: "/commands/format"},
                    {text: "Import", link: "/commands/import"},
                    {text: "Lineage", link: "/commands/lineage"},
                    {text: "Patch", link: "/commands/patch"},
                    {text: "Render", link: "/commands/render"},
                    {text: "Query", link: "/commands/query"},
                    {text: "AI Enhance", link: "/commands/ai-enhance"},
                    {text: "Cloud", link: "/commands/cloud"},
                ],
            },
            {
                text: "Data Governance",
                collapsed: false,
                items: [
                    {text: "Glossary", link: "/getting-started/glossary"},
                    {text: "Policies", link: "/getting-started/policies"},
                    {
                        text: "Quality Checks",
                        collapsed: false,
                        items: [
                            {text: "Overview", link: "/quality/overview"},
                            {text: "Column Checks", link: "/quality/available_checks"},
                            {text: "Custom Checks", link: "/quality/custom"},
                        ],
                    },
                    {text: "Unit Tests", link: "/quality/unit-tests"},
                ],
            },
            {
                text: "Deployment & CI/CD",
                collapsed: false,
                items: [
                    {
                        text: "Deployment",
                        link: "/deployment/overview",
                        collapsed: false,
                        items: [
                            {text: "Ubuntu VM with Cron", link: "/deployment/vm-deployment"},
                            {text: "Apache Airflow", link: "/deployment/airflow"},
                            {text: "GitHub Actions", link: "/deployment/github-actions"},
                            {text: "GitLab CI/CD", link: "/deployment/cloud/gitlab-cicd"},
                            {
                                text: "AWS",
                                collapsed: false,
                                items: [
                                    {text: "Lambda", link: "/deployment/cloud/aws-lambda"},
                                    {text: "ECS", link: "/deployment/cloud/aws-ecs"}
                                ]
                            },
                            {
                                text: "Google Cloud",
                                collapsed: false,
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
                ],
            },
            {
                text: "Secret Providers",
                link: "/secrets/overview",
                collapsed: false,
                items: [
                    {text: "File Backend (.bruin.yml)", link: "/secrets/bruinyml"},
                    {text: "Hashicorp Vault", link: "/secrets/vault"},
                    {text: "Doppler", link: "/secrets/doppler"},
                    {text: "AWS Secrets Manager", link: "/secrets/aws-secrets-manager"},
                ]
            },
            {
                text: "Templates",
                link: "/getting-started/templates",
                collapsed: false,
                items: [
                    {text: "Overview", link: "/getting-started/templates"},
                    {
                        text: "Analytics Demos",
                        collapsed: false,
                        items: [
                            {text: "ecommerce", link: "/getting-started/templates-docs/ecommerce-README"},
                            {text: "demo-snowflake-sales-analytics", link: "/getting-started/templates-docs/demo-snowflake-sales-analytics-README"},
                            {text: "demo-snowflake-salesforce", link: "/getting-started/templates-docs/demo-snowflake-salesforce-README"},
                            {text: "self-heal-demo", link: "/getting-started/templates-docs/self-heal-demo-README"},
                        ],
                    },
                    {
                        text: "Local & Learning",
                        collapsed: false,
                        items: [
                            {text: "duckdb", link: "/getting-started/templates-docs/duckdb-README"},
                            {text: "python", link: "/getting-started/templates-docs/python-README"},
                            {text: "chess", link: "/getting-started/templates-docs/chess-README"},
                            {text: "frankfurter", link: "/getting-started/templates-docs/frankfurter-README"},
                        ],
                    },
                    {
                        text: "Warehouses & Databases",
                        collapsed: false,
                        items: [
                            {text: "athena", link: "/getting-started/templates-docs/athena-README"},
                            {text: "clickhouse", link: "/getting-started/templates-docs/clickhouse-README"},
                            {text: "bronze-silver-postgres", link: "/getting-started/templates-docs/bronze-silver-postgres-README"},
                        ],
                    },
                    {
                        text: "Source Ingestion",
                        collapsed: false,
                        items: [
                            {text: "shopify-bigquery", link: "/getting-started/templates-docs/shopify-bigquery-README"},
                            {text: "shopify-duckdb", link: "/getting-started/templates-docs/shopify-duckdb-README"},
                            {text: "gsheet-bigquery", link: "/getting-started/templates-docs/gsheet-bigquery-README"},
                            {text: "gsheet-duckdb", link: "/getting-started/templates-docs/gsheet-duckdb-README"},
                            {text: "notion", link: "/getting-started/templates-docs/notion-README"},
                            {text: "gorgias", link: "/getting-started/templates-docs/gorgias-README"},
                            {text: "firebase", link: "/getting-started/templates-docs/firebase-README"},
                        ],
                    },
                ],
            },
            {
                text: "Developer Tools",
                collapsed: false,
                items: [
                    {
                        text: "VS Code Extension",
                        link: "/vscode-extension/overview",
                        collapsed: false,
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
                                    {text: "Table Diff", link: "/vscode-extension/panels/table-diff"},
                                    {text: "Run History", link: "/vscode-extension/panels/run-history"},
                                    {text: "Activity Bar", link: "/vscode-extension/panels/activity-bar"},
                                ],
                            },
                        ],
                    },
                    {text: "Bruin MCP", link: "/getting-started/bruin-mcp"},
                ],
            },
        ],

        socialLinks: [{icon: "github", link: "https://github.com/bruin-data/bruin"}],
    },

    markdown: {
        headers: {
            level: [2, 3, 4],
        },
        languages: ["sql", "yaml", "shell", "python", "json", jinja2Grammar, bruinSqlGrammar, bruinPythonGrammar],
    },

    mermaid: {
        // refer https://mermaid.js.org/config/setup/modules/mermaidAPI.html#mermaidapi-configuration-defaults for options
    },
    // optionally set additional config for plugin itself with MermaidPluginConfig
    mermaidPlugin: {
        class: "mermaid my-class", // set additional css classes for parent container
    },
    vite: {
        build: {
            target: ["chrome107", "edge107", "firefox104", "safari16"],
        },
    },
});
