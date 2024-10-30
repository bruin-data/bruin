# Chess
[chess](https://www.chess.com/) is an online platform offering chess games, tournaments, lessons, and more.

ingestr supports Chess as a source for [ingestr assets](https://bruin-data.github.io/bruin/assets/ingestr.html), allowing you to ingest data from Chess into your data warehouse.
It is designed to play around with the data of players, games, and more since it doesn't require any authentication.

In order to have set up Chess connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Chess section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)

```yaml
    connections:
      chess:
        - name: "connection_name"
          players:
            - "MagnusCarlsen"
            - "Hikaru"
