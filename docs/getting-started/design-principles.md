# Design Principles

Bruin is an opinionated data framework, which contains:
- various open-source components,
  - [Bruin CLI](https://github.com/bruin-data/bruin)
  - [ingestr](https://github.com/bruin-data/ingestr)
  - [Bruin VS Code extension](https://github.com/bruin-data/bruin-vscode)
- a framework to build scalable data products, both in terms of data size as well as the _data team size_ 
- and a managed data platform as a service, [Bruin Cloud](https://getbruin.com),

Below is a living list of design principles that outlines the vision behind Bruin, both as an open-source product as well as a business.

---

1. **Everything should be done via some form of a version-controllable text.** This means there should be no UI/database to configure anything in terms of how Bruin pipelines run, which enables proper reproducible builds for data pipelines.
2. **Real-world data pipelines use multiple technologies.** While SQL is incredibly powerful, there are a lot of usecases that are not doable via just SQL. Bruin supports both Python and SQL natively, as well as pre-built images/binaries to support more complex usecases.
3. **Real-world data pipelines depend on multiple sources and destinations.** A complete data platform needs to be able to fulfill the needs of various business functions, and enable creating a single source of truth, be it for marketing platforms, CRM systems, data warehouses, data lakes and many more. Data pipelines must be built to support the diverse set of sources and destinations.
4. **Real-world data pipelines need the ability to mix-and-match technologies, sources, and destinations.** Consequently, it is not enough for a platform to support multiple source and destinations if they cannot be combined. A single pipeline must be able to mix and match technologies, sources and destinations as needed without any disruption.
5. **Avoid lock-in.** Data is core, and lock-in in data should be avoided. This is why Bruin CLI runs on any environment, and supports all the core features out of the box via the open-source, Apache-licensed product.
6. **Anything can be a data asset.** A real-world data pipeline consists of many types of assets: tables, views, spreadsheets, files on S3, an ML model, anything. Anything that creates value with data is an asset, and the data platform should support incorporating different types of assets into the existing workflows gradually.
7. **Data must be treated as a product.** This means providing it as a trustworthy asset to the rest of the organization with the best customer experience by making it discoverable, interoperable, addressable, and self-describing. Taking care of the quality, governance, and maintainability of every data asset is a must for any forward-looking data team, and the tools must be built to support that.
8. **Built-in support for multiple environments is a must.** It should be possible to execute the same pipeline against a production database, as well as a development environment, without having to change the code.
9. **There should be not a single line of glue code.** Building data assets should have just the necessary code, and no glue. No pipelining code, no environment setup, no secret fetching, nothing additional.
10. **Data analysts & data scientists should be able to productionize their assets/pipelines by themselves.** This means taking the complexity away from these folks, and letting them focus on the core business logic.
11. **Data quality is a first-class citizen.** It is not, and it won't be an afterthought. The quality must be a part of every development process.
12. **Quick feedback cycle is key for better data development.** This means every solution we build should have the ability to quickly test things out, and run locally as much as possible. This is why we have a blazing-fast open-source CLI tool, and a local development environment that replicates the production.
13. **Top-notch developer experience is a must.** People that work with these tools may not know the inner-workings, the tools we built must go further in explaining what went wrong, and how to fix it, as well as making the data developers' day-to-day lives easier.

The list of principles here can be considered as a lens to look at [Bruin](https://getbruin.com) as a whole.
