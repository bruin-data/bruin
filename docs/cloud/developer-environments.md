# Developer Environments

Developer Environments are online Integrated Development Environments ([IDEs](https://en.wikipedia.org/wiki/Integrated_development_environment)) that come with your pipelines and Bruin tooling pre-configured.

Author and deploy your pipelines—all without leaving your browser.

<img src="/public/dev-env/demo.png">

## Configuration

In order to use Developer Environments, you need to at least have a Git secret configured.

### Git Secret 

Developer Environments use Personal Access Tokens (PAT) to access your Bruin Git repositories. Follow the steps below to obtain your PAT.

#### Generate Access Token

##### GitHub
- Follow the instructions from the [Official GitHub Docs](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token) to create your personal access token.
- Set an appropriate expiry. If unsure, set it to `Never Expire`.
- Choose `Only select repositories` under `Repository Access` when creating the permission.
- In the `Permissions` section, add `Contents` permission. 
> [!IMPORTANT] Note on Permissions
> `Read Only` access will allow you to only run your pipelines in Developer Environments. If you wish to push changes 
> or deploy pipelines, you will need to give the token `Read and Write` access.
- Click `Generate token` and copy the token value.

##### GitLab
- Follow the instructions from the [Official GitLab Docs](https://docs.gitlab.com/user/profile/personal_access_tokens/#create-a-personal-access-token) to create your personal access token.
- Set an appropriate expiry. If unsure, set it to `Never Expire`.
- Under scopes, select `read_repository` and `write_repository`.
> [!IMPORTANT] Note on Scopes
> The `read_repository` scope will allow you to only run your pipelines in Developer Environments. If you wish to push changes 
> or deploy pipelines, you will need to give the token `write_repository` scope as well.
- Click `Create token` and copy the token value.

#### Add the PAT on Cloud
- Click on `Team Settings` from the Team Selector Dropdown. <img style="padding: 1rem 0" src="/public/dev-env/dropdown-team-settings.png">
- Scroll down to `Git Secrets` section and add:
    - **Token Name**: We recommend giving it the same name as the one you used when generating the token.
    - **Username**: The username of the account that created the token.
    - **Token**: The token itself.

<img style="padding: 1rem 1.5rem" src="/public/dev-env/git-secret.png">

- Click `Save` to complete the process.

Developer Environments are now ready for use.

### Environment Secrets

Your Developer Environments can be pre-configured with [secrets](/secrets/bruinyml.md). This means that when you launch a Developer Environment, you can run your pipelines directly without having to configure individual connections.

> [!NOTE]
> Environment Secrets are user-scoped. This contrasts with Git secrets, which are team-scoped.

To configure environment secrets:
- Click on `Team Settings` from the Team Selector Dropdown. 
- Scroll down to `Developer Environment Secret`. <img style="padding: 1rem 0" src="/public/dev-env/environment-secret.png">
- Paste the contents of your `.bruin.yml` file.
- Click `Save`.

Now, any Developer Environment instances launched by the current user will have the secrets injected into the workspace. 

## Creating a New Developer Environment

> [!TIP] Prerequisite
> Make sure that you've set up your [Git credentials](#git-secret) before you continue.

- Click on `Developer Environments` from the Team Selector Dropdown. <img style="padding: 1rem 0" src="/public/dev-env/dropdown-developer-environments.png">
- Click the `New Instance` button in the top right. <img style="padding: 1rem 0" src="/public/dev-env/new-instance.png">
- Give your instance a name, select the Bruin Git repository you want to launch the instance with, and click `Create Instance`. <img style="padding: 1rem 0" src="/public/dev-env/create-instance-modal.png">
- Click the `Launch` button once the instance status becomes `Active`.

Your Developer Environment should launch shortly. Happy coding!
