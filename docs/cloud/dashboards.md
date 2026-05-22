# Dashboards

A **dashboard** in Bruin Cloud is an interactive canvas of charts, tables, metrics, and text that an [AI agent](/cloud/ai-agents/overview) builds for you from a conversation. You describe what you want, the agent runs the SQL, lays out widgets on a grid, and saves the result. Your team can open the dashboard later without re-asking.

Dashboards live under **AI → Dashboards** in the top nav. You need an agent with a [connection set](/cloud/connections#connection-sets-for-ai-agents) before you can build one.

## Create a dashboard

### 1. Open Dashboards

From the **AI** menu, choose **Dashboards**.

### 2. New Dashboard

Click **New Dashboard**, pick the agent that should build it, and give it an optional name. The agent inherits its project, connection set, and CLI access from your existing configuration.

### 3. Describe what you want

A dashboard opens in **edit mode** with a chat composer at the bottom. Send the agent a prompt like:

> Build a dashboard showing daily revenue, top 10 products by revenue, and a 7-day moving average of order volume.

The agent runs queries, picks widget types, and lays them out on a row/column grid. You can iterate — ask for changes, swap chart types, reorder rows. Each message the agent sends updates the canvas in real time.

### 4. Publish

When the layout looks right, click **Publish**. The current draft becomes the published version that everyone with access sees. The dashboard exits edit mode.

You can re-enter **Edit** at any time to keep iterating. While unpublished changes exist, the header shows **Unpublished changes**; click **Discard** to roll back to the last published version.

## Widgets

The agent can place four widget types on the canvas:

| Type | What it shows |
|---|---|
| **Chart** | SQL-backed visualisation (line, bar, pie, area, etc.) |
| **Metric** | Single KPI value with optional trend |
| **Table** | Rows from a SQL query |
| **Text** | Markdown blocks — section headers, narrative, links |

Widgets are arranged in rows. Each row can hold multiple widgets side by side. The agent decides the layout; you can ask it to rearrange.

## Threads and chat history

Each dashboard has a chat panel and **per-user threads**. Your conversations with the agent are private — a teammate viewing the same dashboard sees its canvas but not your chats. They have their own threads to ask follow-up questions.

Click the thread tabs above the composer to switch between past conversations. From the menu on a tab you can **Rename** or **Delete** a thread.

## Sharing and access

By default a dashboard is **private** to its creator. Open the **Share** dialog to change that.

**Visibility:**

- **Private** — only people you explicitly add can see it.
- **Team** — everyone on your team can view; only editors can publish changes.

**Per-user roles** (admins and the creator can grant):

- **Viewer** — can see the published version, ask the agent questions in their own thread.
- **Editor** — can also edit the canvas and publish.

The creator and team admins are always editors. Removing the agent's access (or losing it on your account) hides the chat sidebar but keeps the rendered widgets visible.

## Edit mode and concurrency

Only one editor can write to the draft at a time. If a teammate's agent is mid-update, sending a message returns *"{name}'s agent is currently working on this dashboard. Please wait."* The lock clears when their agent finishes.

Agents in **view mode** can still answer questions about the data — they just can't modify the canvas. Writes from a view-mode session are silently dropped.

## Limits

- **Free tier:** dashboard and thread creation count against the AI usage limits described in [Pay-as-you-go](/cloud/insights#usage). Creating a new thread when the cap is reached returns an error.
- **Attachments:** up to **5 files per message, 100 MB each**. Files upload to S3 and are referenced as message attachments.
- **One active editor per dashboard** at a time, enforced server-side.

## Delete a dashboard

Open the dashboard menu and pick **Delete**. Only the creator or a team admin can delete. Deleting removes the dashboard, all threads, and any uploaded attachments (subject to S3 lifecycle rules).

## Related

- [Configure Agents](/cloud/ai-agents/configure) — set up the agent that builds the dashboard.
- [Chat with Agents](/cloud/ai-agents/chat) — iterate on questions outside a dashboard before committing to a layout.
- [Connection Sets](/cloud/connections#connection-sets-for-ai-agents) — control which data the agent can read while building.
