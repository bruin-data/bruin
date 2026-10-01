/* @bruin
name: quickbooks_stage.accounts
type: bq.sql
description: >
  The QuickBooks chart of accounts, one row per account, with the financial
  statement each account reports on and its parent account resolved. Use it to
  label and group transaction lines by account.

materialization:
  type: table

depends:
  - quickbooks_raw.accounts

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - accounts

columns:
  - name: account_id
    type: STRING
    description: QuickBooks account identifier.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: account_name
    type: STRING
    description: Account name.
    checks:
      - name: not_null
  - name: fully_qualified_name
    type: STRING
    description: Account name prefixed by its parent accounts, separated by `:`.
  - name: account_number
    type: STRING
    description: User-defined account number.
  - name: account_type
    type: STRING
    description: QuickBooks account type, for example `Bank`, `Expense`, or `Income`.
    checks:
      - name: not_null
  - name: account_sub_type
    type: STRING
    description: More specific category within the account type, for example `Checking`.
  - name: classification
    type: STRING
    description: '`Asset`, `Liability`, `Equity`, `Revenue`, or `Expense`.'
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Asset
          - Liability
          - Equity
          - Revenue
          - Expense
  - name: financial_statement
    type: STRING
    description: >
      Statement the account reports on: `balance_sheet` for asset, liability,
      and equity accounts, `income_statement` for revenue and expense accounts.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - balance_sheet
          - income_statement
  - name: parent_account_id
    type: STRING
    description: Parent account of a sub-account; null for top-level accounts.
  - name: is_sub_account
    type: BOOL
    description: Whether the account is a sub-account of another account.
  - name: description
    type: STRING
    description: Account description.
  - name: currency
    type: STRING
    description: Account currency code, for example `USD`.
  - name: current_balance
    type: NUMERIC
    description: >
      Account balance QuickBooks reported at the last load, excluding
      sub-accounts. QuickBooks only maintains it for balance sheet accounts.
  - name: current_balance_with_sub_accounts
    type: NUMERIC
    description: Account balance including sub-accounts, at the last load.
  - name: is_active
    type: BOOL
    description: Whether the account is active in QuickBooks.
  - name: created_at
    type: TIMESTAMP
    description: When the account was created in QuickBooks.
  - name: updated_at
    type: TIMESTAMP
    description: When the account was last modified in QuickBooks.
    checks:
      - name: not_null
unit_tests:
  - name: maps_classification_to_statement
    inputs:
      - asset: quickbooks_raw.accounts
        rows:
          - id: "1"
            name: "Checking"
            classification: Asset
            fully_qualified_name: null
            account_type: null
            account_sub_type: null
            acct_num: null
            description: null
            parent_ref: null
            sub_account: null
            active: null
            current_balance: null
            current_balance_with_sub_accounts: null
            currency_ref: null
            meta_data: null
            lastupdatedtime: null
          - {id: "2", name: "Subscription Revenue", classification: Revenue}
          - {id: "3", name: "AWS", classification: Expense, sub_account: true, parent_ref: {value: "9", name: "Hosting"}}
          - {id: "4", name: "SAFE Investments", classification: Equity}
    expected:
      match: exact
      rows:
        - {account_id: "1", financial_statement: balance_sheet, is_sub_account: false, is_active: true}
        - {account_id: "2", financial_statement: income_statement}
        - {account_id: "3", financial_statement: income_statement, parent_account_id: "9", is_sub_account: true}
        - {account_id: "4", financial_statement: balance_sheet}
@bruin */

SELECT
  id AS account_id,
  name AS account_name,
  fully_qualified_name,
  acct_num AS account_number,
  account_type,
  account_sub_type,
  classification,
  IF(
    classification IN ('Revenue', 'Expense'),
    'income_statement',
    'balance_sheet'
  ) AS financial_statement,
  JSON_VALUE(parent_ref, '$.value') AS parent_account_id,
  IFNULL(sub_account, FALSE) AS is_sub_account,
  description,
  JSON_VALUE(currency_ref, '$.value') AS currency,
  ROUND(CAST(current_balance AS NUMERIC), 2) AS current_balance,
  ROUND(CAST(current_balance_with_sub_accounts AS NUMERIC), 2) AS current_balance_with_sub_accounts,
  IFNULL(active, TRUE) AS is_active,
  TIMESTAMP(JSON_VALUE(meta_data, '$.CreateTime')) AS created_at,
  lastupdatedtime AS updated_at
FROM quickbooks_raw.accounts;
