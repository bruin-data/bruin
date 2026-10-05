/* @bruin
name: quickbooks_reports.expense_review_queue
type: bq.sql
owner: finance@example.com
description: >
  Spend lines that need a bookkeeper's attention, one row per flagged line,
  with every reason it was flagged and, where the payee's history allows, a
  suggested account. Lines are flagged when they are uncategorized, look
  like a duplicate of another transaction, are much larger than the payee's
  recent lines on the same account, are coded to an account the payee is
  rarely coded to, or are the first spend with a new vendor. Uncategorized
  lines are flagged whatever their date; the other flags only apply to lines
  in the last `review_window_days` days (90 by default) before the run's end
  date, so old, already-reviewed lines drop off. The checks are heuristics
  meant for review, not proof of an error, and balance sheet lines such as
  card payoffs are included.

materialization:
  type: table

depends:
  - quickbooks_stage.expense_lines

tags:
  - quickbooks_reports
  - quickbooks
  - accounting
  - expenses
  - review

domains:
  - finance

meta:
  grain: one row per flagged purchase or bill line
  answers: >
    What needs categorizing? Are there duplicate charges? Which charges
    look wrong or unusual this month?
  contains_pii: "true"
  pii_note: Payees and descriptions can name people.
  agent_notes: >
    Work the queue newest first and read each description before acting.
    suggested_account_name reflects the payee's history only; vendors that
    sell more than one thing (a cloud vendor's conference, a ride app's food
    delivery) are legitimately coded to several accounts. Never change the
    books without approval: propose recategorizations and duplicate deletions
    for a person to apply in QuickBooks.

columns:
  - name: source_type
    type: STRING
    description: '`purchase` or `bill`.'
    primary_key: true
    checks:
      - name: not_null
  - name: transaction_id
    type: STRING
    description: QuickBooks purchase or bill identifier.
    primary_key: true
    checks:
      - name: not_null
  - name: line_index
    type: INT64
    description: Position of the line within the transaction.
    primary_key: true
    checks:
      - name: not_null
  - name: transaction_date
    type: DATE
    description: Date of the purchase or bill.
    checks:
      - name: not_null
  - name: payee_id
    type: STRING
    description: Payee; null when none was recorded.
  - name: payee_name
    type: STRING
    description: Payee display name.
  - name: payment_type
    type: STRING
    description: '`Cash`, `Check`, `CreditCard`, or `Bill`.'
  - name: paid_from_account_name
    type: STRING
    description: Bank or card account paid from, or the A/P account for bills.
  - name: account_id
    type: STRING
    description: Account the line is coded to now.
  - name: account_name
    type: STRING
    description: Name of the account the line is coded to now.
  - name: description
    type: STRING
    description: Line description, often the bank or card statement text.
  - name: amount
    type: NUMERIC
    description: Line amount.
    checks:
      - name: not_null
  - name: review_reasons
    type: STRING
    description: >
      Comma-separated reasons: `uncategorized`, `possible_duplicate`,
      `amount_spike`, `unusual_account`, `new_vendor`.
    checks:
      - name: not_null
  - name: is_uncategorized
    type: BOOL
    description: The line sits in a placeholder account.
    checks:
      - name: not_null
  - name: is_possible_duplicate
    type: BOOL
    description: >
      Another transaction has the same payee (or the same description when
      there is no payee) and amount within 7 days before it.
    checks:
      - name: not_null
  - name: duplicate_of_transaction_id
    type: STRING
    description: The earlier transaction this line may duplicate.
  - name: is_amount_spike
    type: BOOL
    description: >
      The transaction's total on this account is at least 1.75 times, and at
      least 500 more than, the average of the payee's previous three
      transactions on the same account.
    checks:
      - name: not_null
  - name: typical_amount
    type: NUMERIC
    description: >
      Average of the payee's previous three transactions on the same account,
      to compare with the transaction's total on this account.
  - name: is_unusual_account
    type: BOOL
    description: >
      The payee's other lines (at least 3) are coded to one account at
      least 75% of the time, and this line is coded elsewhere.
    checks:
      - name: not_null
  - name: is_new_vendor
    type: BOOL
    description: >
      The line is the first spend with a payee first seen in the last
      `review_window_days` days.
    checks:
      - name: not_null
  - name: suggested_account_id
    type: STRING
    description: >
      Account the payee's other lines are most often coded to, when this line
      is coded elsewhere or uncategorized, the payee has at least 2 other
      categorized lines, and at least half of them use that account.
  - name: suggested_account_name
    type: STRING
    description: Name of the suggested account.
  - name: suggestion_confidence
    type: FLOAT64
    description: Share of the payee's other categorized lines coded to the suggested account, from 0 to 1.
    checks:
      - name: min
        value: 0
      - name: max
        value: 1

custom_checks:
  - name: review line keys are unique
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
  - name: flags_duplicates_spikes_and_miscoding
    description: >
      A repeated charge within a week, a jump against the payee's last three
      transactions, an uncategorized line with a confident suggestion, a line
      coded away from the payee's usual account, and each payee's first line
      flagged as a new vendor inside a window that covers every fixture date.
    variables:
      review_window_days: 100000
    inputs:
      - asset: quickbooks_stage.expense_lines
        rows:
          - {source_type: purchase, transaction_id: t1, line_index: 0, transaction_date: "2025-01-03", payee_id: v1, payee_name: Figma, payment_type: CreditCard, paid_from_account_name: Card, account_id: software, account_name: Software, description: Figma, amount: 90, is_uncategorized: false}
          - {source_type: purchase, transaction_id: t2, line_index: 0, transaction_date: "2025-02-03", payee_id: v1, account_id: software, account_name: Software, amount: 90, is_uncategorized: false}
          - {source_type: purchase, transaction_id: t3, line_index: 0, transaction_date: "2025-03-03", payee_id: v1, account_id: software, account_name: Software, amount: 90, is_uncategorized: false}
          - {source_type: purchase, transaction_id: t4, line_index: 0, transaction_date: "2025-03-04", payee_id: v1, account_id: software, account_name: Software, amount: 90, is_uncategorized: false}
          - {source_type: purchase, transaction_id: u1, line_index: 0, transaction_date: "2025-05-01", payee_id: v1, account_id: uncat, account_name: Uncategorized Expense, amount: 45, is_uncategorized: true}
          - {source_type: purchase, transaction_id: a1, line_index: 0, transaction_date: "2025-01-02", payee_id: v2, account_id: hosting, account_name: Hosting, amount: 1000, is_uncategorized: false}
          - {source_type: purchase, transaction_id: a2, line_index: 0, transaction_date: "2025-02-02", payee_id: v2, account_id: hosting, account_name: Hosting, amount: 1000, is_uncategorized: false}
          - {source_type: purchase, transaction_id: a3, line_index: 0, transaction_date: "2025-03-02", payee_id: v2, account_id: hosting, account_name: Hosting, amount: 1000, is_uncategorized: false}
          - {source_type: purchase, transaction_id: a4, line_index: 0, transaction_date: "2025-04-02", payee_id: v2, account_id: hosting, account_name: Hosting, amount: 3000, is_uncategorized: false}
          - {source_type: purchase, transaction_id: a5, line_index: 0, transaction_date: "2025-05-02", payee_id: v2, account_id: software, account_name: Software, amount: 300, is_uncategorized: false}
    expected:
      match: exact
      rows:
        - {transaction_id: t1, review_reasons: new_vendor, is_new_vendor: true}
        - {transaction_id: a1, review_reasons: new_vendor, is_new_vendor: true}
        - {transaction_id: t4, review_reasons: possible_duplicate, is_possible_duplicate: true, duplicate_of_transaction_id: t3}
        - {transaction_id: a4, review_reasons: amount_spike, is_amount_spike: true, typical_amount: 1000}
        - {transaction_id: u1, review_reasons: uncategorized, suggested_account_id: software, suggestion_confidence: 1}
        - {transaction_id: a5, review_reasons: unusual_account, is_unusual_account: true, suggested_account_id: hosting, suggestion_confidence: 1}
@bruin */

WITH lines AS (
  SELECT
    *,
    COALESCE(payee_id, CONCAT('description:', UPPER(TRIM(description)))) AS payee_key,
    CONCAT(source_type, ':', transaction_id) AS transaction_key
  FROM quickbooks_stage.expense_lines
  WHERE transaction_date <= DATE('{{ end_date }}')
),

duplicates AS (
  SELECT
    line.source_type,
    line.transaction_id,
    line.line_index,
    ARRAY_AGG(earlier.transaction_id ORDER BY earlier.transaction_date DESC LIMIT 1)[OFFSET(0)] AS duplicate_of_transaction_id
  FROM lines AS line
  JOIN lines AS earlier
    ON earlier.payee_key = line.payee_key
    AND earlier.amount = line.amount
    AND earlier.transaction_key != line.transaction_key
    AND earlier.transaction_date BETWEEN DATE_SUB(line.transaction_date, INTERVAL 7 DAY) AND line.transaction_date
    AND (
      earlier.transaction_date < line.transaction_date
      OR earlier.transaction_key < line.transaction_key
    )
  GROUP BY 1, 2, 3
),

-- Spikes compare whole transactions per payee and account, so a payroll run
-- with wage and tax lines on one account is compared run to run.
transaction_totals AS (
  SELECT
    payee_key,
    account_id,
    transaction_key,
    MIN(transaction_date) AS transaction_date,
    SUM(amount) AS transaction_amount
  FROM lines
  GROUP BY 1, 2, 3
),

trailing AS (
  SELECT
    payee_key,
    account_id,
    transaction_key,
    transaction_amount,
    AVG(transaction_amount) OVER recent AS typical_amount,
    COUNT(*) OVER recent AS prior_transactions
  FROM transaction_totals
  WINDOW recent AS (
    PARTITION BY payee_key, account_id
    ORDER BY transaction_date, transaction_key
    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
  )
),

payee_accounts AS (
  SELECT payee_key, account_id, ANY_VALUE(account_name) AS account_name, COUNT(*) AS line_count
  FROM lines
  WHERE NOT is_uncategorized AND account_id IS NOT NULL
  GROUP BY 1, 2
),

payee_totals AS (
  SELECT payee_key, SUM(line_count) AS categorized_lines
  FROM payee_accounts
  GROUP BY payee_key
),

dominant_accounts AS (
  SELECT payee_key, account_id, account_name, line_count
  FROM payee_accounts
  QUALIFY ROW_NUMBER() OVER (PARTITION BY payee_key ORDER BY line_count DESC, account_id) = 1
),

payee_first_lines AS (
  SELECT source_type, transaction_id, line_index, transaction_date
  FROM lines
  WHERE payee_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payee_id
    ORDER BY transaction_date, transaction_id, line_index
  ) = 1
),

flagged AS (
  SELECT
    line.*,
    duplicates.duplicate_of_transaction_id,
    trailing.typical_amount,
    duplicates.transaction_id IS NOT NULL AS is_possible_duplicate,
    IFNULL(
      trailing.prior_transactions = 3
        AND trailing.transaction_amount >= 1.75 * trailing.typical_amount
        AND trailing.transaction_amount - trailing.typical_amount >= 500,
      FALSE
    ) AS is_amount_spike,
    dominant.account_id AS dominant_account_id,
    dominant.account_name AS dominant_account_name,
    -- Share of the payee's other categorized lines on the dominant account.
    SAFE_DIVIDE(
      dominant.line_count - IF(line.account_id = dominant.account_id AND NOT line.is_uncategorized, 1, 0),
      totals.categorized_lines - IF(line.is_uncategorized OR line.account_id IS NULL, 0, 1)
    ) AS dominant_share,
    totals.categorized_lines - IF(line.is_uncategorized OR line.account_id IS NULL, 0, 1) AS other_categorized_lines,
    IFNULL(
      first_lines.transaction_id IS NOT NULL
        AND first_lines.transaction_date > DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.review_window_days }} DAY),
      FALSE
    ) AS is_new_vendor
  FROM lines AS line
  LEFT JOIN duplicates USING (source_type, transaction_id, line_index)
  LEFT JOIN trailing
    ON line.payee_key = trailing.payee_key
    AND line.account_id IS NOT DISTINCT FROM trailing.account_id
    AND line.transaction_key = trailing.transaction_key
  LEFT JOIN dominant_accounts AS dominant
    ON line.payee_key = dominant.payee_key
  LEFT JOIN payee_totals AS totals
    ON line.payee_key = totals.payee_key
  LEFT JOIN payee_first_lines AS first_lines
    USING (source_type, transaction_id, line_index)
),

scored AS (
  SELECT
    *,
    IFNULL(
      NOT is_uncategorized
        AND account_id != dominant_account_id
        AND other_categorized_lines >= 3
        AND dominant_share >= 0.75,
      FALSE
    ) AS is_unusual_account,
    IFNULL(
      (is_uncategorized OR account_id != dominant_account_id)
        AND other_categorized_lines >= 2
        AND dominant_share >= 0.5,
      FALSE
    ) AS has_suggestion
  FROM flagged
)

SELECT
  source_type,
  transaction_id,
  line_index,
  transaction_date,
  payee_id,
  payee_name,
  payment_type,
  paid_from_account_name,
  account_id,
  account_name,
  description,
  amount,
  ARRAY_TO_STRING(
    ARRAY(
      SELECT reason
      FROM UNNEST([
        IF(is_uncategorized, 'uncategorized', NULL),
        IF(is_possible_duplicate, 'possible_duplicate', NULL),
        IF(is_amount_spike, 'amount_spike', NULL),
        IF(is_unusual_account, 'unusual_account', NULL),
        IF(is_new_vendor, 'new_vendor', NULL)
      ]) AS reason
      WHERE reason IS NOT NULL
    ),
    ', '
  ) AS review_reasons,
  is_uncategorized,
  is_possible_duplicate,
  duplicate_of_transaction_id,
  is_amount_spike,
  ROUND(typical_amount, 2) AS typical_amount,
  is_unusual_account,
  is_new_vendor,
  IF(has_suggestion, dominant_account_id, NULL) AS suggested_account_id,
  IF(has_suggestion, dominant_account_name, NULL) AS suggested_account_name,
  IF(has_suggestion, ROUND(dominant_share, 2), NULL) AS suggestion_confidence
FROM (
  SELECT
    * REPLACE (
      is_possible_duplicate AND in_review_window AS is_possible_duplicate,
      is_amount_spike AND in_review_window AS is_amount_spike,
      is_unusual_account AND in_review_window AS is_unusual_account
    )
  FROM (
    SELECT
      *,
      transaction_date > DATE_SUB(DATE('{{ end_date }}'), INTERVAL {{ var.review_window_days }} DAY) AS in_review_window
    FROM scored
  )
)
WHERE is_uncategorized
  OR is_possible_duplicate
  OR is_amount_spike
  OR is_unusual_account
  OR is_new_vendor;
