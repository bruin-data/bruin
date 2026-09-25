# Sources and privacy

Use public content by default and collect only content your team is authorised to access. Reddit uses a compliant OAuth or public API path; Hacker News uses Algolia HN Search. Keep API windows, pages, rates and deletion handling within each provider’s current terms.

GitHub activity, Stack Exchange and Slack community data require public/user-authorised access and their respective APIs. LinkedIn, X and Quora are supported only through authorised APIs, user-provided exports or approved providers. This template does not scrape, evade access controls, collect private messages, enrich profiles by default, or infer sensitive traits.

For a deletion/redaction request, record the source and external ID, honour the provider’s deletion process, remove or tombstone matching raw records as policy requires, rebuild dependent warehouse data, and retain only a minimal audit record. Mark missing or stale source data in downstream reports and notifications.
