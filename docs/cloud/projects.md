# Projects

A [project](/core-concepts/project) in Bruin Cloud has a one-to-one relationship with a Git repository. Creating a project connects your repo, syncs its pipelines, and gives you a place to manage the [connections](/cloud/connections) and [secrets](/secrets/overview) those pipelines need. The project structure itself (the `bruin.yml`, `pipeline.yml`, and `assets/` layout) is defined in the CLI — see [Project](/core-concepts/project) for the layout and [`bruin init`](/commands/init) for scaffolding a new one.

You can create projects during onboarding, or any time afterward from the home page.

## Create a project

### 1. Start a new project

If you skipped adding a project during onboarding and have no projects yet, click **Create project** on the home page. Otherwise, open **Team settings → Projects** and use the **New project** button.

### 2. Connect GitHub

Authenticate GitHub using one of:

- **Bruin GitHub App** — recommended. Fine-grained access, no expiring tokens, and you can grant access on a per-repo basis from the GitHub UI.
- **Personal access token** — works fine, but tokens expire and live on a single user.

### 3. Select a repo

Once GitHub is connected, pick the repo you want to set up as a new project. Each project maps to exactly one repo.

### 4. Name the project

Rename the project or leave it matching the repo name, then click **Create project**.

### 5. Wait for the sync

Bruin Cloud syncs the pipelines in your repo. This can take a few minutes. You can keep working while it runs. Start adding connections in parallel.

### 6. Add connections

While the project is syncing, head to [Connections](/cloud/connections) and add the data sources, destinations, and secrets your pipelines reference.

## GitHub authentication

Bruin Cloud supports two ways to authenticate with GitHub.

### Bruin GitHub App (recommended)

- Installed once per GitHub organization (or personal account).
- Scoped to specific repositories.
- No expiring tokens.
- Once installed, every new project you create lists the repos the app has access to, no per-project setup.

To grant access to more repos later, open the [Bruin Cloud GitHub App page](https://github.com/apps/bruin-cloud) on GitHub and click **Configure**. (You can also reach it from GitHub: **Settings → Applications → Bruin Cloud → Configure**.)

### Personal access token (PAT)

- A single user's token used to authenticate Bruin Cloud against your repo.
- Tokens expire and are tied to the user who created them. If that user leaves, the project loses access.

We recommend migrating PAT-based projects to the GitHub App.

## Migrate a project to the GitHub App

If you have existing projects authenticated with a personal access token, you can move them to the Bruin GitHub App in under a minute, directly from **Team Settings**.

### 1. Open Team Settings

Click your team name in the top bar and choose **Team Settings**.

### 2. Go to the Projects section

In Team Settings, open the **Projects** tab from the left sidebar. This is where every project connected to your team lives.

### 3. Click "Migrate to GitHub App"

Scroll down to your existing projects. Any project still using a PAT shows a **Migrate to GitHub App** action. Click it to start.

> [!TIP]
> If the Bruin GitHub App is already installed on the organization that owns the repo, the migration finishes immediately. The next two steps are skipped.

### 4. Choose where to install the app

You will be redirected to GitHub. Pick the account or organization that owns the repository you are migrating.

### 5. Grant access to your repositories

Choose **All repositories** to give the app access to every repo, or **Only select repositories** to pick specific ones. Click **Install** to confirm.

We recommend granting access only to the repositories you actually use with Bruin. You can grant more later from GitHub.

### 6. Back to Bruin

After you confirm on GitHub, you are redirected back to Bruin and the migration finishes automatically. The migrated project no longer shows the **Migrate to GitHub App** action. That is how you know it is using the GitHub App now.

## Adding a new project from a different repo

Once the GitHub App is installed, every new project you create can use it. The **Connect with GitHub** option lists every repo the app has access to.

If the repo you want is not in the list, grant the Bruin GitHub App access to it on GitHub first:

1. Open the [Bruin Cloud GitHub App page](https://github.com/apps/bruin-cloud) and click **Configure**.
2. Pick the installation (account or organization) where the app lives.
3. Under **Repository access**, click **Select repositories** and add the new repo. Hit **Save**.
4. Back in Bruin Cloud, the newly granted repo appears automatically in the **Connect with GitHub** list. No reinstall needed.

## Next

- [Connections](/cloud/connections) for setting up the data sources and secrets the project's pipelines need.
- [Pipelines](/cloud/pipelines) for enabling and operating the pipelines once they finish syncing.
- [Project structure](/core-concepts/project) — repo layout and config files the cloud sync expects.
- [`bruin init`](/commands/init) — scaffold a new Bruin project locally.
- [`bruin validate`](/commands/validate) — verify pipelines and assets before pushing.
