/* @bruin
name: quickbooks_stage.expense_lines
type: bq.sql
description: >
  Lines from purchases (cash expenses, checks, and card charges) and vendor
  bills in one table, one row per line, with the payee and the account each
  line is categorized to resolved. Card refunds carry a negative amount. Lines
  can post to balance sheet accounts as well as expenses, for example a check
  that pays down a credit card or loan, an owner's draw, or a capitalized
  purchase, so filter on `is_expense` for profit-and-loss spend; otherwise
  card payoffs double count card charges. Item-based lines have a null
  `is_expense` because their item account is not loaded.
  Vendor credits are not loaded, so they are not netted. Lines are dated by
  their transaction, so bills count when billed, not when paid.

materialization:
  type: table

depends:
  - quickbooks_raw.purchases
  - quickbooks_raw.bills
  - quickbooks_stage.accounts

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - expenses
  - purchases
  - bills

columns:
  - name: source_type
    type: STRING
    description: '`purchase` for lines from purchases, `bill` for lines from vendor bills.'
    primary_key: true
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - purchase
          - bill
  - name: transaction_id
    type: STRING
    description: >
      Parent purchase or bill identifier; joins to `quickbooks_raw.purchases`
      or `quickbooks_stage.bills` depending on `source_type`.
    primary_key: true
    checks:
      - name: not_null
  - name: line_index
    type: INT64
    description: Zero-based position of the line in the transaction's line array.
    primary_key: true
    checks:
      - name: not_null
  - name: line_id
    type: STRING
    description: QuickBooks line identifier within the transaction.
  - name: line_type
    type: STRING
    description: >
      `account_based` for lines categorized directly to an account,
      `item_based` for lines that record a product or service item, which post
      to the item's expense or inventory account.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - account_based
          - item_based
  - name: transaction_date
    type: DATE
    description: Date of the purchase or bill.
    checks:
      - name: not_null
  - name: payment_type
    type: STRING
    description: >
      How the spend was paid: `Cash`, `Check`, or `CreditCard` for purchases,
      and `Bill` for vendor bills.
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - Cash
          - Check
          - CreditCard
          - Bill
  - name: paid_from_account_id
    type: STRING
    description: >
      Bank or credit card account a purchase was paid from, or the accounts
      payable account a bill posts to.
  - name: paid_from_account_name
    type: STRING
    description: Name of the paid-from account.
  - name: payee_id
    type: STRING
    description: >
      Payee of the transaction. For bills and vendor payees it joins to
      `quickbooks_stage.vendors`; check `payee_type`.
  - name: payee_name
    type: STRING
    description: Payee display name at the time of the last load.
  - name: payee_type
    type: STRING
    description: '`Vendor`, `Customer`, or `Employee`; null when no payee was recorded.'
  - name: account_id
    type: STRING
    description: >
      Account the line is categorized to; joins to `quickbooks_stage.accounts`.
      Null for item-based lines, which post to the item's expense account.
      `account_type` and `account_classification` are null when the account
      was made inactive, because inactive accounts are not loaded.
  - name: account_name
    type: STRING
    description: Name of the categorized account.
  - name: account_type
    type: STRING
    description: Type of the categorized account, for example `Expense` or `Cost of Goods Sold`.
  - name: account_classification
    type: STRING
    description: >
      Classification of the categorized account. `Expense` for profit-and-loss
      spend; `Asset`, `Liability`, or `Equity` for capitalized purchases, debt
      or card payoffs, and owner's draws.
  - name: is_expense
    type: BOOL
    description: >
      Whether the line counts as profit-and-loss spend: true for lines on an
      `Expense` account, false for lines on balance sheet accounts, such as
      card or loan payoffs, owner's draws, and capitalized purchases. Null for
      item-based lines, whose item account is not loaded, so they can be
      either an expense or an inventory purchase; review them separately.
  - name: is_uncategorized
    type: BOOL
    description: >
      Whether the line sits in a placeholder account (`Uncategorized Expense`,
      `Uncategorized Asset`, or `Ask My Accountant`) and still needs a category.
    checks:
      - name: not_null
  - name: item_id
    type: STRING
    description: Item on item-based expense lines; null for account-based lines.
  - name: description
    type: STRING
    description: Line description, often the bank or card statement text.
  - name: amount
    type: NUMERIC
    description: >
      Line amount before tax, in the transaction currency. Negative for credit
      card refunds and credits.
    checks:
      - name: not_null
  - name: currency
    type: STRING
    description: Transaction currency code, for example `USD`.
  - name: is_billable
    type: BOOL
    description: Whether the line is marked billable to a customer.
    checks:
      - name: not_null
  - name: billable_customer_id
    type: STRING
    description: Customer the line is rebillable to, when set.
  - name: memo
    type: STRING
    description: Internal memo of the parent transaction.

custom_checks:
  - name: expense line keys are unique
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT source_type, transaction_id, line_index
        FROM {{ this }}
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
      )
    value: 0
unit_tests:
  - name: signs_and_flags_purchase_lines
    description: >
      Card charges are positive, card credits negative, card payoffs are not
      expenses, item-based lines are left unclassified, and placeholder
      accounts are flagged.
    inputs:
      - asset: quickbooks_raw.purchases
        rows:
          - id: "p1"
            txn_date: "2026-03-02"
            payment_type: CreditCard
            credit: false
            account_ref: {value: "card", name: "Brex Card"}
            entity_ref: {value: "v1", name: "Figma", type: "Vendor"}
            currency_ref: {value: "USD"}
            private_note: null
            line:
              - {Id: "1", Amount: 45.5, DetailType: AccountBasedExpenseLineDetail, AccountBasedExpenseLineDetail: {AccountRef: {value: "software"}}}
              - {Amount: 10, DetailType: SubTotalLineDetail, SubTotalLineDetail: {}}
          - id: "p2"
            txn_date: "2026-03-05"
            payment_type: CreditCard
            credit: true
            account_ref: {value: "card", name: "Brex Card"}
            line:
              - {Id: "1", Amount: 20, DetailType: AccountBasedExpenseLineDetail, AccountBasedExpenseLineDetail: {AccountRef: {value: "software"}}}
          - id: "p3"
            txn_date: "2026-03-06"
            payment_type: Check
            account_ref: {value: "bank", name: "Checking"}
            line:
              - {Id: "1", Amount: 500, DetailType: AccountBasedExpenseLineDetail, AccountBasedExpenseLineDetail: {AccountRef: {value: "card"}}}
              - {Id: "2", Amount: 80, DetailType: ItemBasedExpenseLineDetail, ItemBasedExpenseLineDetail: {ItemRef: {value: "item9"}, BillableStatus: Billable, CustomerRef: {value: "c7"}}}
              - {Id: "3", Amount: 12, DetailType: AccountBasedExpenseLineDetail, AccountBasedExpenseLineDetail: {AccountRef: {value: "uncat"}}}
      - asset: quickbooks_stage.accounts
        rows:
          - {account_id: "software", account_name: "Software", account_type: "Expense", classification: "Expense"}
          - {account_id: "card", account_name: "Brex Card", account_type: "Credit Card", classification: "Liability"}
          - {account_id: "uncat", account_name: "Uncategorized Expense", account_type: "Expense", classification: "Expense"}
    expected:
      match: exact
      rows:
        - {source_type: purchase, transaction_id: "p1", line_index: 0, line_type: account_based, payee_id: "v1", payee_type: Vendor, account_name: Software, amount: 45.5, is_expense: true, is_uncategorized: false, is_billable: false}
        - {source_type: purchase, transaction_id: "p2", line_index: 0, line_type: account_based, amount: -20, is_expense: true, is_uncategorized: false}
        - {source_type: purchase, transaction_id: "p3", line_index: 0, line_type: account_based, account_classification: Liability, amount: 500, is_expense: false, is_uncategorized: false}
        - {source_type: purchase, transaction_id: "p3", line_index: 1, line_type: item_based, account_id: null, item_id: "item9", amount: 80, is_expense: null, is_billable: true, billable_customer_id: "c7"}
        - {source_type: purchase, transaction_id: "p3", line_index: 2, line_type: account_based, amount: 12, is_expense: true, is_uncategorized: true}
  - name: bill_lines_are_positive_vendor_spend
    inputs:
      - asset: quickbooks_raw.bills
        rows:
          - id: "b1"
            txn_date: "2026-04-01"
            vendor_ref: {value: "v2", name: "Pilot"}
            ap_account_ref: {value: "ap", name: "Accounts Payable (A/P)"}
            currency_ref: null
            private_note: null
            line:
              - {Id: "1", Amount: 900, DetailType: AccountBasedExpenseLineDetail, AccountBasedExpenseLineDetail: {AccountRef: {value: "acct"}}}
      - asset: quickbooks_stage.accounts
        rows:
          - {account_id: "acct", account_name: "Accounting Fees", account_type: "Expense", classification: "Expense"}
    expected:
      match: exact
      rows:
        - {source_type: bill, transaction_id: "b1", payment_type: Bill, paid_from_account_id: "ap", payee_id: "v2", payee_type: Vendor, amount: 900, is_expense: true}
@bruin */

WITH purchase_lines AS (
  SELECT
    'purchase' AS source_type,
    purchase.id AS transaction_id,
    line_index,
    line,
    purchase.txn_date AS transaction_date,
    purchase.payment_type,
    JSON_VALUE(purchase.account_ref, '$.value') AS paid_from_account_id,
    JSON_VALUE(purchase.account_ref, '$.name') AS paid_from_account_name,
    JSON_VALUE(purchase.entity_ref, '$.value') AS payee_id,
    JSON_VALUE(purchase.entity_ref, '$.name') AS payee_name,
    JSON_VALUE(purchase.entity_ref, '$.type') AS payee_type,
    JSON_VALUE(purchase.currency_ref, '$.value') AS currency,
    IF(IFNULL(purchase.credit, FALSE), -1, 1) AS sign,
    purchase.private_note AS memo
  FROM quickbooks_raw.purchases AS purchase
  CROSS JOIN UNNEST(
    IFNULL(JSON_QUERY_ARRAY(purchase.line), ARRAY<JSON>[])
  ) AS line WITH OFFSET AS line_index
),

bill_lines AS (
  SELECT
    'bill' AS source_type,
    bill.id AS transaction_id,
    line_index,
    line,
    bill.txn_date AS transaction_date,
    'Bill' AS payment_type,
    JSON_VALUE(bill.ap_account_ref, '$.value') AS paid_from_account_id,
    JSON_VALUE(bill.ap_account_ref, '$.name') AS paid_from_account_name,
    JSON_VALUE(bill.vendor_ref, '$.value') AS payee_id,
    JSON_VALUE(bill.vendor_ref, '$.name') AS payee_name,
    'Vendor' AS payee_type,
    JSON_VALUE(bill.currency_ref, '$.value') AS currency,
    1 AS sign,
    bill.private_note AS memo
  FROM quickbooks_raw.bills AS bill
  CROSS JOIN UNNEST(
    IFNULL(JSON_QUERY_ARRAY(bill.line), ARRAY<JSON>[])
  ) AS line WITH OFFSET AS line_index
),

all_lines AS (
  SELECT * FROM purchase_lines
  UNION ALL
  SELECT * FROM bill_lines
),

typed_lines AS (
  SELECT
    all_lines.*,
    JSON_VALUE(line, '$.DetailType') AS detail_type,
    COALESCE(
      JSON_QUERY(line, '$.AccountBasedExpenseLineDetail'),
      JSON_QUERY(line, '$.ItemBasedExpenseLineDetail')
    ) AS detail
  FROM all_lines
)

SELECT
  line.source_type,
  line.transaction_id,
  line.line_index,
  JSON_VALUE(line.line, '$.Id') AS line_id,
  IF(line.detail_type = 'ItemBasedExpenseLineDetail', 'item_based', 'account_based') AS line_type,
  line.transaction_date,
  line.payment_type,
  line.paid_from_account_id,
  line.paid_from_account_name,
  line.payee_id,
  line.payee_name,
  line.payee_type,
  JSON_VALUE(line.detail, '$.AccountRef.value') AS account_id,
  COALESCE(account.account_name, JSON_VALUE(line.detail, '$.AccountRef.name')) AS account_name,
  account.account_type,
  account.classification AS account_classification,
  CASE
    WHEN line.detail_type = 'ItemBasedExpenseLineDetail' THEN NULL
    ELSE IFNULL(account.classification = 'Expense', FALSE)
  END AS is_expense,
  IFNULL(
    COALESCE(account.account_name, JSON_VALUE(line.detail, '$.AccountRef.name'))
      IN ('Uncategorized Expense', 'Uncategorized Asset', 'Ask My Accountant'),
    FALSE
  ) AS is_uncategorized,
  JSON_VALUE(line.detail, '$.ItemRef.value') AS item_id,
  JSON_VALUE(line.line, '$.Description') AS description,
  ROUND(SAFE_CAST(JSON_VALUE(line.line, '$.Amount') AS NUMERIC) * line.sign, 2) AS amount,
  line.currency,
  IFNULL(JSON_VALUE(line.detail, '$.BillableStatus') IN ('Billable', 'HasBeenBilled'), FALSE) AS is_billable,
  JSON_VALUE(line.detail, '$.CustomerRef.value') AS billable_customer_id,
  line.memo
FROM typed_lines AS line
LEFT JOIN quickbooks_stage.accounts AS account
  ON JSON_VALUE(line.detail, '$.AccountRef.value') = account.account_id
WHERE line.detail_type IN ('AccountBasedExpenseLineDetail', 'ItemBasedExpenseLineDetail');
