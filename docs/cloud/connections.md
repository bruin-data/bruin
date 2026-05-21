# Connections

[Connections](/connections/overview) are named configurations that let Bruin authenticate and talk to external systems — data sources, destinations, and any other platform a pipeline or agent depends on.

Locally, you define connections in `.bruin.yml`. That file should be gitignored — it holds [secrets](/core-concepts/secrets#secrets) and credentials. In Bruin Cloud, you configure the same connections through the UI. They need to match the names referenced in your pipelines, and they are encrypted at rest using [HashiCorp Vault](/secrets/vault) as the secrets backend.

## Add a connection

### 1. Spot what is missing

Open a pipeline. On the right-hand side, Bruin Cloud lists the connections it uses. Any connection referenced in your pipelines but not configured in Bruin Cloud is flagged so you know what to fix.

You can also work from a clean slate: open **Manage team → Connections**.

### 2. Open Connections and click "New connection"

From **Manage team**, go to **Connections** and click **New connection**.

### 3. Pick the connection type

Select the type that matches your data platform — BigQuery, Postgres, Snowflake, Databricks, and so on — or pick a **generic secret** for API keys, tokens, and other text-based credentials. See [Platforms](/platforms/aws) for the full list of supported types.

### 4. Match the name

The connection name must match the one referenced in your pipeline exactly. If they do not match, the pipeline will keep flagging the connection as missing.

### 5. Fill in the details

Enter the rest of the connection details. Each platform asks for different fields — access key, host, project ID, username and password, and so on.

### 6. Create and validate

Click **Create**. For most data platforms, Bruin Cloud validates the connection by running a `SELECT 1` against the database. If you do not want that, choose **Create without validation**.

### 7. Confirm it is active

Go back to your pipeline. The connection should appear in the right-hand list without the missing flag.

## Generic secrets

For text-based credentials that are not tied to a specific data platform (API keys, OAuth tokens, third-party secrets), use the **generic secret** connection type. These work the same way as named connections in `.bruin.yml` — they are referenced by name from your pipelines and assets.

## Connection sets (for AI agents)

A **connection set** is a named bundle of connections used by an [AI agent](/cloud/ai-agents/overview). Connection sets are kept separate from the connections your pipelines use, which lets you:

- Restrict agents to only the data they need.
- Give agents read-only access where pipelines have read/write.
- Apply granular, agent-specific permissions without touching pipeline credentials.

To create a connection set, open **Connections** in team settings, click **New connection set**, give it a name, and pick the data platform connections it should include. Then attach it to an agent from the [agent configuration page](/cloud/ai-agents/configure).
