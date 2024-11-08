---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Bruin"
  text: "End-to-end data framework"
  tagline: Built-in data ingestion, transformation, data quality, governance.
  actions:
    - theme: brand
      text: Getting Started
      link: /getting-started/introduction
    - theme: alt
      text: Design Principles
      link: /getting-started/concepts/design-principles
    - theme: alt
      text: GitHub
      link: https://github.com/bruin-data/bruin
  image:
    src: /BruinLogo-square.svg
    alt: Bruin

features:
  - title: Data Ingestion
    icon: 📥
    details: Bruin allows you to ingest data from many sources into many destinations with a simple YAML file.
  - title: Transformation
    icon: 🔄
    details: Bruin enables you to transform your data using SQL and Python without any custom code.    
  - title: Data Quality
    icon: ✅
    details: Bruin contains built-in data quality checks that can be used to validate your data. You can also write custom checks.
  - title: VS Code Extension
    icon: 💻
    details: On top of the CLI, we built a VS Code extension to allow you to easily work on your pipelines.
---


<style>
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