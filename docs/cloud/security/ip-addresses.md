# Bruin's IP Addresses

Bruin Cloud can set up dedicated egress IP addresses for customers that need to restrict access to private systems. This lets your security team allowlist a known IP address instead of opening access to the wider internet.

Your dedicated egress IP can be used by Bruin Cloud when it needs to reach systems such as:

- Git providers, to clone and sync your repositories.
- Databases and data warehouses, to run assets and validate connections.
- Internal APIs, object stores, and other network-restricted services used by your pipelines.

## How It Works

Dedicated egress IPs are configured per customer. Once the IP is provisioned, your account manager will share it with you directly. You can then add that IP to the allowlist, firewall rule, network policy, or access control list for the systems Bruin Cloud needs to reach.

The IP address is dedicated to your team and is not shared with other customers. Because these addresses are customer-specific, Bruin does not publish a public list of Cloud egress IPs.

## Setup Process

1. Contact your account manager and request a dedicated egress IP for Bruin Cloud.
2. Bruin provisions an IP address dedicated to your team.
3. Your account manager shares the IP address with your team.
4. You allowlist the IP address in your Git provider, database, warehouse, firewall, or other protected service.
5. Bruin Cloud uses that IP for outbound access to the systems covered by the setup.

## When To Use This

Use a dedicated egress IP when your organization requires network-level allowlisting for:

- Private Git repositories.
- Databases or warehouses that reject unknown source IPs.
- Internal services used by Python assets or ingestion jobs.
- Vendor APIs or storage systems protected by IP restrictions.

If you are unsure whether your project needs a dedicated egress IP, contact your account manager with the systems Bruin Cloud needs to access and the allowlisting requirements from your security team.
