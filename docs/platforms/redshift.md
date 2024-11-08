# AWS Redshift

Bruin supports AWS Redshift as a data warehouse, which means you can use Bruin to build tables and views in your Redshift data warehouse.

## Connection
In order to have set up a Redshift connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema
Mind that, despite the connection being at all effects a Postgres connection, the default `port` field of Amazon Redshift is `5439`.

```yaml
    connections:
      redshift:
        - name: "connection_name"
          username: "awsuser"
          password: "XXXXXXXXXX"
          host: "redshift-cluster-1.xxxxxxxxx.eu-north-1.redshift.amazonaws.com"
          port: 5439
          database: "dev"
          ssl_mode: "allow"
```

> [!NOTE]
> `ssl_mode` should be one of the modes describe in the [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION).


### Making Redshift publicly accessible

Before the connection works properly, you need to ensure that the Redshift cluster can be access from the outside. In order to do that you must mark the configuration option in your redshift cluster

![Make publicly available](/publicly-accessible.png)

In addition to this, you must configure the inbound rules of the security group your redshift cluster belongs to, to accept inbound connections. In the example below we enabled access for all origins but you can set more restrictive rules for this.

![Inbound Rules](/inbound-rules.png)

If you have trouble setting this up you can check [AWS documentation](https://repost.aws/knowledge-center/redshift-cluster-private-public) on the topic


## AWS Redshift Assets

### `rs.sql`
Runs a materialized AWS Redshift asset or an SQL script. For detailed parameters, you can check [Definition Schema](definition-schema.md) page.

#### Example: Create a table for product reviews
```sql
/* @bruin
name: product_reviews.table
type: rs.sql
materialization:
    type: table
@bruin */

create table product_reviews (
    review_id bigint identity(1,1),
    product_id bigint,
    user_id bigint,
    rating int,
    review_text varchar(500),
    review_date timestamp
);
```

#### Example: Run an AWS Redshift script to clean up old data
```sql
/* @bruin
name: clean_old_data
type: rs.sql
@bruin */

begin transaction;

delete from user_activity
where activity_date < dateadd(year, -2, current_date);

delete from order_history
where order_date < dateadd(year, -5, current_date);

commit transaction;
```
