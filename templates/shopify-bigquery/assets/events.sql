/* @bruin

name: events
type: bq.sql

materialization:
   type: table

description: |
  # Products table
  This asset represents the Shopify products data.

  You can write Markdown here, it supports `inline codeblocks` or larger blocks of code. It supports **bold** and *italic* text.

depends:
  - shopify_raw.events

columns:
  - name: id
    type: integer
    description: "Event unique identifier"
    primary_key: true
    checks:
        - name: not_null

  - name: subject_id
    type: integer
    description: "ID of the subject this event relates to"

  - name: created_at
    type: timestamp
    description: "When the event was created"

  - name: subject_type
    type: string
    description: "Type of the subject this event relates to"

  - name: verb
    type: string
    description: "Action performed in this event"

  - name: arguments
    type: json
    description: "Additional event arguments"

  - name: message
    type: string
    description: "Event message"

  - name: author
    type: string
    description: "Who created this event"

  - name: description
    type: string
    description: "Detailed description of the event"

@bruin */

SELECT * from `shopify_raw.price_rules`;
