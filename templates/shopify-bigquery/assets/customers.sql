/* @bruin

name: customers
type: bq.sql

materialization:
   type: table

description: |
  # Example table
  This asset is an example table with some quality checks to help you get started.

  You can write Markdown here, it supports `inline codeblocks` or larger blocks of code. It supports **bold** and *italic* text.

depends:
  - shopify_raw.customers

columns:
  - name: id
    type: integer
    description: "Customer unique identifier"
    primary_key: true
    checks:
        - name: not_null
        - name: positive

  - name: email
    type: varchar
    description: "Customer email address"
    checks:
        - name: not_null

  - name: created_at
    type: timestamp
    description: "Timestamp when customer was created"
    checks:
        - name: not_null

  - name: updated_at
    type: timestamp
    description: "Timestamp when customer was last updated"

  - name: first_name
    type: varchar
    description: "Customer first name"

  - name: last_name
    type: varchar
    description: "Customer last name"

  - name: orders_count
    type: integer
    description: "Total number of orders placed by customer"
    checks:
        - name: positive

  - name: state
    type: varchar
    description: "Customer account state"

  - name: total_spent
    type: varchar
    description: "Total amount spent by customer"

  - name: verified_email
    type: boolean
    description: "Whether customer email is verified"

  - name: tax_exempt
    type: boolean
    description: "Whether customer is tax exempt"

  - name: tags
    type: varchar
    description: "Customer tags"

  - name: currency
    type: varchar
    description: "Customer's preferred currency"

  - name: phone
    type: varchar
    description: "Customer phone number"

  - name: addresses
    type: json
    description: "Customer addresses"

  - name: tax_exemptions
    type: json
    description: "Customer tax exemptions"

  - name: email_marketing_consent
    type: json
    description: "Customer email marketing consent information"

  - name: sms_marketing_consent
    type: json
    description: "Customer SMS marketing consent information"

  - name: admin_graphql_api_id
    type: varchar
    description: "Admin GraphQL API identifier"

  - name: note
    type: varchar
    description: "Customer note"

  - name: default_address
    type: json
    description: "Customer default address"


@bruin */

SELECT * from `shopify_raw.customers`;