# Developer Environments

Developer Environments are online Integrated Development Environments ([IDE](https://en.wikipedia.org/wiki/Integrated_development_environment)) that come with your pipelines and bruin tooling pre-configured.

Author and deploy your pipelines, all without leaving your browser.

## Configuration

In order to use developer environments, you need to atleast have a git secret configured.

### Git Secret 

Developer Environments use Personal Access Token (PAT) to access your bruin git repositories. Follow the steps below to obtain your PAT.

#### Generate Access Token
---

##### Github
- Follow the instructions from the [Offical Github Docs](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token) to create your personal access token.
- Set an appropriate expiry. If unsure, set it to `Never Expire`.
- Choose `Only select repositories` under `Repository Access` when creating the permission.
- In `Permissions` section, add `Contents` permission. 
> [!IMPORTANT] Note on Permisions
> A `Read Only` access will allow you to only run your pipelines in Developer Environments. If you wish to push changes 
> or deploy pipelines, you will need to give the token `Read and Write` access.
- Click `Generate token` and copy the token value.

##### Gitlab
---
- Follow the instructions from the [Offical Gitlab Docs](https://docs.gitlab.com/user/profile/personal_access_tokens/#create-a-personal-access-token) to create your personal access token.
- Set an appropriate expiry. If unsure, set it to `Never Expire`.
- Under scopes select `read_repository` and `write_repository`.
> [!IMPORTANT] Note on scopes
> A `read_repository` scope will allow you to only run your pipelines in Developer Environments. If you wish to push changes 
> or deploy pipelines, you will need to give the token `write_repository` scope as well.
- Click `Create token` and copy the token value.

#### Add the PAT on Cloud
---
- Click on `Team Settings` from the Team Selector Dropdown. <p>![Dropdown](/public/dev-env/dropdown-team-settings.png)</p>
- Scroll Down to `Git Secrets` Section and add
    - Token Name. We recommended giving it the same name as the one you used when generating the token.
    - Username of the account that created the token.
    - Token itself
<p>

![Git-Secret](/public/dev-env/git-secret.png)
</p>

- Click `Save` to complete the process.

Developer Environments are now ready for use.

### Pipeline Secrets

## Usage