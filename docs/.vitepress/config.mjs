import { defineConfig } from "vitepress";

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Bruin CLI",
  description: "Open-source multi-language data pipelines",
  themeConfig: {
    nav: [{ text: "Home", link: "/" }],
    sidebar: [
      {
        text: "Getting Started",
        items: [{ text: "Introduction", link: "/getting-started/introduction" }],
      },
    ],

    socialLinks: [{ icon: "github", link: "https://github.com/bruin-data/bruin" }],
  },
});
