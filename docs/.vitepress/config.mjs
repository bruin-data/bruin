import {defineConfig} from "vitepress";

// https://vitepress.dev/reference/site-config
export default defineConfig({
    title: "Bruin CLI",
    description: "Open-source multi-language data pipelines",
    base: '/bruin/',
    themeConfig: {
        nav: [{text: "Home", link: "/"}],
        sidebar: [
            {
                text: "Getting Started",
                items: [
                    {text: "Introduction", link: "/getting-started/introduction"},
                    {text: "Concepts", link: "/getting-started/concepts"}
                ],
            },
            {
                text: "Connections",
                items: [
                    {
                        text: "Overview", link: "/connections/overview", items: [

                            {text: "Redshift", link: "/connections/redshift"},
                            {text: "Postgres", link: "/connections/postgres"},
                            {text: "Google Cloud Platform", link: "/connections/google_cloud_platform"},
                            {text: "Microsoft SQL Server", link: "/connections/mssql"},
                            {text: "MySQL", link: "/connections/mysql"},
                            {text: "Snowflake", link: "/connections/snowflake"},
                        ]
                    },
                ],
            },
            {
                text: "Assets",
                items: [
                    {text: "Definition Schema", link: "/assets/definition-schema"},
                ],
            },
            {
                text: "Templating",
                items: [
                    {text: "Overview", link: "/templating/templating"},
                    {text: "Filters", link: "/templating/filters"},
                ],
            },
        ],

        socialLinks: [{icon: "github", link: "https://github.com/bruin-data/bruin"}],
    },
});
