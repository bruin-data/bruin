---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Bruin"
  text: "Multi-language data pipelines"
  tagline: Bruin is an open-source command-line application that enables data analysts and data scientists to build multi-language data pipelines.
  actions:
    - theme: brand
      text: Getting Started
      link: /getting-started/introduction
    - theme: alt
      text: Design Principles
      link: /getting-started/concepts/design-principles
  image:
    src: /BruinLogo-square.svg
    alt: Bruin

features:
  - title: SQL & Python Transformations
    details: Bruin enables you to transform your data using SQL and Python without any custom code.
  - title: Built-in data quality checks
    details: Bruin contains built-in data quality checks that can be used to validate your data, whether it's generated by SQL or Python. You can write custom checks in SQL as well.
  - title: Automated Materialization
    details: Just write the `SELECT` query and let Bruin take care of building the tables and views for you. It handles incremental updates as well as full refreshes.
---

<style>
:root {
  --vp-home-hero-name-color: transparent;
  --vp-home-hero-name-background: linear-gradient(to right, rgb(217, 95, 95), rgb(206, 50, 50));;
  
}

@media (min-width: 640px) {
  :root {
    --vp-home-hero-image-filter: blur(56px);
  }
}

@media (min-width: 960px) {
  :root {
    --vp-home-hero-image-filter: blur(68px);
  }
}

:root {
  --vp-c-brand-1: #d95f5f;
}
</style>