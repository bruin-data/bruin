# MySQL

In order to have set up a MySQL connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mysql:
        - name: "connection_name"
          username: "mysql_user"
          password: "XXXXXXXXXX"
          host: "mysql_host.somedomain.com"
          port: 3306
          database: "dev"
```
