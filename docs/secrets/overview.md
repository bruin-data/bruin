# Secret Providers

Bruin CLI normally retrieves secrets to instantiate connections to different platforms based on your local [`.bruin.yml`](./bruinyml), but it's also possible to use secret management solutions to provide these secrets.

For a complete guide on managing your project, environments, and secrets, see the [Project](/core-concepts/project) documentation.

At the moment, Bruin supports the following:

* [.bruin.yml](./bruinyml) - File-based configuration (default)
* [Hashicorp Vault](./vault)
* [Doppler](./doppler)
* [AWS Secrets Manager](./aws-secrets-manager)
