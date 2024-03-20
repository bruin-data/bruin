import {defineConfig} from "vitepress";

// https://vitepress.dev/reference/site-config
export default defineConfig({
    title: "Bruin CLI",
    description: "Open-source multi-language data pipelines",
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
                    {text: "Redshift", link: "/connections/redshift"},
                    {text: "Postgres", link: "/connections/postgres"}
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
