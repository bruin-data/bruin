# sftp

SFTP (SSH File Transfer Protocol) is a secure file transfer protocol that runs over the SSH protocol. It provides a secure way to transfer files between a local and a remote computer.

Bruin supports sftp as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from sftp into your data warehouse.

In order to set up an SFTP connection, add a connection in `.bruin.yml` and an asset file. You need the server's `username`, `host`, and `port`, plus a password or a private-key file.

Follow the steps below to correctly set up sftp as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to sftp, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    sftp:
        - name: sftp
          username: user_1
          password: pass-1234
          host: localhost
          port: 22
```

- `username`: The username for the SFTP server.
- `password`: The password for the SFTP server. When `key_file` is set, this is instead the private-key passphrase; password authentication is not also attempted.
- `host`: The hostname or IP address of the SFTP server.
- `port`: The port number of the SFTP server.

#### SSH private keys and server verification

```yaml
connections:
    sftp:
        - name: registry-sftp
          username: bulk-user
          host: sftp.example.gov
          port: 22
          key_file: /run/secrets/sftp_key
          known_hosts_file: /run/secrets/known_hosts
```

The key file must be readable on the machine running Bruin. Ingestr supports RSA, Ed25519, and ECDSA keys in the formats supported by its SSH library. For encrypted keys, supply the passphrase through `password` using your connection's secrets backend rather than committing it to YAML.

| Optional field | Description |
| --- | --- |
| `key_file` | Path to a private key. Use an absolute path; `~` and environment variables are not expanded by ingestr for this field. |
| `key_passphrase` | Legacy passphrase option. Used only when `password` is empty and `key_file` is set. |
| `known_hosts_file` | OpenSSH known-hosts file. Ingestr expands `~` and `~/` in this path. Unknown hosts and changed keys fail verification. |
| `host_key_fingerprint` | List of accepted `SHA256:` fingerprints. Takes precedence over `known_hosts_file`. |
| `insecure_skip_host_key_check` | Set to `true` to disable server verification, overriding both verification options. Intended only for local testing. |

To pin fingerprints instead of using a known-hosts file, replace `known_hosts_file` with:

```yaml
          host_key_fingerprint:
            - "SHA256:<base64 fingerprint supplied by the server operator>"
```

Fingerprints must use unpadded Base64 SHA256 digests. Multiple entries allow a planned host-key rotation: verify the new fingerprint with the server operator, add it alongside the old one, and remove the old fingerprint after the rotation.

::: warning Server verification is opt-in
For backward compatibility, ingestr warns but accepts any server key when neither `known_hosts_file` nor `host_key_fingerprint` is configured. This also applies to key-based authentication. Always configure one of these options in production. Do not use `insecure_skip_host_key_check` to work around a changed host key; verify the change with the server operator first.
:::

For onboarding, send only your **public** key to the server operator and obtain the server's host key or fingerprint over a trusted channel. Keep the private key in a restricted file on the execution machine. Do not trust an unverified `ssh-keyscan` result as proof of server identity. To rotate a client key, arrange for the operator to authorize the new public key, replace the local private-key file, verify access, then revoke the old public key.

::: warning Secret transport limitations
Bruin marks passwords and passphrases as sensitive for log masking, and reads `key_file` contents for masking on used connections. Private-key contents are not placed in the URI. However, ingestr currently receives passwords and passphrases in its source URI, which is passed as a process argument; these values can also be written by the explicit `ingestr-uri` export command. This integration does not provide secret-free URI or process-argument transport. SSH-agent authentication, inline private-key content, and ordered identity lists are not supported.
:::

### Step 2: Create an asset file for data ingestion

To ingest data from sftp, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., sftp_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.sftp
type: ingestr
connection: neon

parameters:
  source_connection: sftp
  source_table: 'users.csv'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the sftp connection defined in .bruin.yml.
- `source_table`: The source-table specifies /path/to/directory. The base directory on the server where bruin should start looking for files.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/sftp_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given sftp table into your Postgres database.

<img alt="sftp" src="./media/sftp_ingestion.png">
