# Databricks Per-User OAuth

Connect Databricks to Bruin with per-user OAuth: create a Databricks OAuth app once, register it in Bruin, and let every user query as themselves. No shared service credential — each query runs with that user's own Unity Catalog grants.

**Audience:** Databricks account admins setting up the OAuth app, and end users connecting their accounts.

**Prerequisites**

- Databricks account admin permissions (to create the OAuth app connection).
- A Bruin Cloud workspace with access to [AI Agent Settings](/cloud/ai-agents/configure).

## Set up the OAuth app (admin, one-time)

### 1. Open the Databricks account console

In your Databricks workspace, click the workspace switcher in the top-right corner and select **Manage account**. OAuth apps are created at the account level, not inside a workspace.

![Databricks workspace switcher with Manage account](/cloud/databricks-oauth/01-manage-account.png)

### 2. Go to account Settings

The account console opens at `accounts.cloud.databricks.com`. Click the **Settings** card.

![Databricks account console](/cloud/databricks-oauth/02-account-console.png)

### 3. Create the OAuth app connection

Under **Settings → App connections**, click **Add connection**:

- **Application Name**: any name, e.g. `bruin`.
- **Redirect URLs**: `https://cloud.getbruin.com/auth/databricks/callback`
- **Access scopes**: check **SQL**. "All APIs" is not needed.
- **Client secret**: enable **Generate a client secret**.
- Leave the access/refresh token TTLs at their defaults.

Click **Add**, then copy the **Client ID** and **Client Secret** — the secret cannot be viewed again. Also grab your **Account ID** from the avatar menu in the top-right.

![Add connection form in Databricks](/cloud/databricks-oauth/03-add-oauth-app.png)

### 4. Register the app in Bruin

In Bruin, open your AI Agent Settings and add a Databricks app with the Account ID, Client ID, and Client Secret. The app is account-level: one app covers all your Databricks workspaces.

![Databricks app registered in Bruin](/cloud/databricks-oauth/04-bruin-add-app.png)

### 5. Copy your workspace URL

Back in the Databricks account console, open **Workspaces** and select the workspace you want to query. Copy its per-workspace URL (it looks like `https://dbc-xxxx.cloud.databricks.com`). Which workspace to query is set per connection.

![Workspace URL in Databricks account console](/cloud/databricks-oauth/05-workspace-url.png)

### 6. Add a Databricks connection to your agent

In your agent's [Connection Set](/cloud/connections#connection-sets-for-ai-agents), click **Add Connection** and choose **Databricks (per-user OAuth)** as the connection type. Name the connection, pick the OAuth app you registered, paste the workspace URL, and optionally set a default catalog.

![Add Databricks per-user OAuth connection in Bruin](/cloud/databricks-oauth/06-add-connection.png)

## Connect your account (each user)

Setup is done — from here, each user links their own Databricks identity once.

### Connect in chat

When you open a [chat](/cloud/ai-agents/chat) with an agent that uses a per-user OAuth connection, a banner asks you to connect. Click **Connect** to start the Databricks OAuth flow and authorize Bruin.

![Connect Databricks banner in chat](/cloud/databricks-oauth/07-connect-prompt-chat.png)

After authorizing, the app shows as **Connected**. You can **Reconnect** if your token expires or **Disconnect** at any time.

![Connected status](/cloud/databricks-oauth/08-connected.png)

### Connect from Slack

The same flow works in [Slack](/cloud/integrations/slack). The first time you mention the agent in a channel backed by a per-user OAuth connection, it replies with a **Connect Databricks** button.

![Connect Databricks button in Slack](/cloud/databricks-oauth/09-slack-connect.png)

Once connected, just mention the agent and ask — queries run against Databricks as you, and the answer lands in the thread.

![Agent answering in Slack](/cloud/databricks-oauth/10-slack-answer.png)

### Scheduled agents

[Scheduled agents](/cloud/ai-agents/scheduled) run with their owner's Databricks connection. If the owner hasn't connected yet, the agent is paused and shows a warning banner — click **Connect** to authorize, and the schedule resumes automatically.

![Scheduled agent waiting for Databricks connection](/cloud/databricks-oauth/11-scheduled-agent.png)

### Dashboards

[Dashboards](/cloud/dashboards) built on a per-user OAuth connection load data as the viewer. A viewer who hasn't connected Databricks sees a **Connect Databricks** banner instead of someone else's data.

![Dashboard asking viewer to connect Databricks](/cloud/databricks-oauth/12-dashboard-connect.png)

Once connected, the dashboard loads with the viewer's own permissions, and the side chat is available for follow-up questions.

![Dashboard loaded with data](/cloud/databricks-oauth/13-dashboard-loaded.png)
