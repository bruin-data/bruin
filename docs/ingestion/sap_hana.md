# SAP HANA

In order to have set up a SAP HANA connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.


```yaml
    connections:
      hana:
        - name: "connection_name"
          username: "hana_user"
          password: "XXXXXXXXXX"
          host: "hana-xyz.sap.com"
          port: 39013
          database: "systemdb"
```
