# Design Principles

Bruin is an opinionated data tool, and in some sense it can be considered as a framework to build scalable data products, both in terms of data size as well as the _data team size_. Below is a living list of design principles that outlines the vision behind Bruin, both as an open-source product as well as a business.

1. **Everything must be done via some form of a version-controllable text.** This means there should be no UI/database to configure anything in terms of how Bruin pipelines run.
2. **You need multiple technologies.** While SQL is incredibly powerful, there are a lot of usecases that are not doable via just SQL. Bruin supports both Python and SQL natively, as well as pre-built images/binaries to support more complex usecases.
3. **You need multiple sources and destinations.** Any company above a certain size would use more than one platform that has relevant data. This can be marketing platforms, CRM systems, data warehouses, data lakes, whatever. Data pipelines must be built to support the diverse set of sources and destinations.
4. **You need the ability to mix-and-match technologies, sources, and destinations.** Consequently, it is not enough for a platform to support multiple source and destinations if they cannot be combined. A single pipeline must be able to mix and match technologies, sources and destinations as needed without any disruption.
5. **Avoid lock-in.** Data is core, and lock-in in data should be avoided. This is why Bruin CLI runs on any environment, and supports all the core features out of the box via the open-source, Apache-licensed product.
6. **Anything can be a data asset.** A real-world data pipeline consists of many types of assets: tables, views, spreadsheets, files on S3, an ML model, anything. Anything that creates value with data is an asset, and the data platform should support incorporating different types of assets into the existing workflows gradually.
7. **Data must be treated as a product.** This means taking care of the quality, governance, and maintainability of every data asset is a must for any forward-looking data team, and the tools must be built to support that.
8. **Built-in support for multiple environments is a must.** It should be possible to execute the same pipeline against a production database, as well as a development environment, without having to change the code.
9. **There should be not a single line of glue code.** Building data assets should have just the necessary code, and no glue. No pipelining code, no environment setup, no secret fetching, nothing additional.
10. **Data analysts & data scientists should be able to productionize their assets/pipelines by themselves.** This means taking the complexity away from these folks, and letting them focus on the core business logic.
11. **Data quality is a first-class citizen.** It is not, and it won't be an afterthought. The quality must be a part of every development process.
12. **Quick feedback cycle is key for better data development.** This means every solution we build should have the ability to quickly test things out, and run locally as much as possible. This is why we have a blazing-fast open-source CLI tool, and a local development environment that replicates the production.
13. **Clear error communication is a must.** People that work with these tools may not know the inner-workings, the tools we built must go further in explaining what went wrong, and how to fix it.

The list of principles here will grow over time, and can be considered as a lens to look at [Bruin](https://getbruin.com) as a whole.
