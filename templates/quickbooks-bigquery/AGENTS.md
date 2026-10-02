# QuickBooks finance agent guide

This pipeline turns QuickBooks Online into BigQuery tables an AI agent can use to answer finance questions and help review the books. This guide says which table answers which question, how the numbers are defined, and how to run the common workflows. Every table and column is also documented in its asset file and in BigQuery column descriptions.

## Ground rules

- **Read only.** Query BigQuery with `bruin query --connection gcp-default --query "..."`; never write to it, and never change QuickBooks. Propose recategorizations, duplicate deletions, mapping changes, and follow-ups for a person to apply. Edit `account_mapping.csv` or run the pipeline only when the user asks.
- **Use the reports first.** Start from `quickbooks_reports`; drop to `quickbooks_stage` for line-level detail. Avoid `quickbooks_raw`, which holds nested JSON.
- **Complete months only.** Month-based reports stop at the last complete month. Say which month a number is for, and say when `monthly_kpis.is_preliminary` is true: that month's bills and card charges may still be coming in.
- **Accrual basis.** Revenue counts on the invoice date and costs on the transaction or bill date. Cash collected is in `monthly_kpis.cash_collected` and `monthly_collections`.
- **Estimates are estimates.** `net_burn` and `runway_months` are approximations; say so when you report them.
- **Not every transaction is loaded.** Only invoices, payments, purchases, and bills come in. Journal entries (often payroll), sales receipts and deposits (often how Stripe syncs), credit memos, and vendor credits are missing. If payroll, revenue, or burn looks off, say this may be why and suggest checking the QuickBooks Profit and Loss report.
- **Category names come from the mapping.** Report lines such as `Travel & Meals` are defined in `account_mapping.csv`, so list `DISTINCT category` (or `primary_category`) before filtering on one.
- **One currency.** Amounts assume the company's single currency.

## Which table answers what

| Question | Table | How |
| --- | --- | --- |
| What's our MRR, ARR, growth, churn? | `quickbooks_reports.monthly_kpis` | Latest `month` row; `mrr`, `arr`, `mrr_growth_rate`, `customer_churn_rate` |
| Who expanded, contracted, or churned? | `quickbooks_reports.customer_mrr_movements` | Filter `month` and `movement` |
| Who are our biggest customers? | `quickbooks_reports.customer_concentration` | Order by `mrr_rank`; `cumulative_share` at rank 10 is top-10 concentration |
| What's our runway? How much cash do we have? | `quickbooks_reports.cash_runway` | One row: `cash_balance`, `avg_monthly_net_burn`, `runway_months` |
| What did we spend on X last month? | `quickbooks_reports.monthly_pnl` | Filter `month` and `category`; drill into `quickbooks_stage.expense_lines` by `account_name`, `payee_name`, or `description` |
| What is our gross margin, opex, net income? | `quickbooks_reports.monthly_kpis` | `gross_margin`, `operating_expenses`, `net_income` |
| Who owes us money? What's overdue? | `quickbooks_reports.ar_aging` | Sum `open_balance` by customer; filter `days_overdue > 0` |
| How fast do customers pay? | `quickbooks_reports.monthly_collections` | `dso_days`, `avg_days_to_pay`, `on_time_payment_rate` |
| Which vendors or SaaS tools do we pay for? | `quickbooks_reports.vendor_spend` | `primary_category = 'Software'` (with the shipped mapping) and `is_recurring`; `estimated_annual_cost` |
| What needs categorizing or looks wrong? | `quickbooks_reports.expense_review_queue` | Filter `review_reasons` |
| How is each account mapped to the P&L? | `quickbooks_stage.account_categories` | `is_mapped = false` means a default line was used |

Spend on a specific event or trip, for example "travel for Snowflake Summit", usually spans several report lines. Search `quickbooks_stage.expense_lines` where `is_expense` and `description` or `memo` mention the event, around its dates, and group by `account_name`.

## Definitions

- **MRR**: recurring revenue invoiced to a customer in the month, from accounts marked `is_recurring_revenue` in `account_mapping.csv`. Annual invoices show as a one-month spike.
- **Movements**: `opening` (first month of data), `new`, `expansion`, `contraction`, `churn` (MRR went to 0), `reactivation`, `retained`.
- **Net burn**: cost of revenue plus operating and other expenses, less cash collected from customers and other income. Positive means spending more than coming in.
- **Runway**: bank balance divided by the average net burn of the last three complete months. Null when not burning, 0 when cash is gone.
- **DSO**: month-end receivables divided by the month's invoicing, times the days in the month.
- **Spend**: `expense_lines` where `is_expense` is true. Card payoffs and other balance sheet postings are excluded; item-based lines have a null `is_expense`, are left out of the P&L, and need a look on their own.
- **MRR caveats**: MRR is before discounts, and sub-customers roll up to their parent. Annual or quarterly billing shows as churn followed by reactivation, so check a customer's invoices before calling it churn.

## Workflow: month-end review

1. Pick the month: the latest `month` in `quickbooks_reports.monthly_kpis`.
2. Summarize the month: MRR and net new MRR with the bridge (`new_mrr`, `expansion_mrr`, `reactivation_mrr`, `contraction_mrr`, `churned_mrr`), revenue, gross margin, operating expenses, net burn, and runway from `cash_runway`. Note if the month `is_preliminary`.
3. Compare costs with the prior month in `monthly_pnl` by `category`, and explain the largest changes with the lines behind them in `expense_lines`.
4. List the review queue for the month (`expense_review_queue` where `transaction_date` is in the month), grouped by reason.
5. List overdue receivables from `ar_aging` with customer, invoice, days overdue, and amount, largest first.
6. Note new and churned customers from `customer_mrr_movements`, and vendors first paid in the last 90 days from `vendor_spend` where `is_new_vendor`.
7. End with the open items a person needs to act on.

## Workflow: categorize transactions

1. Read the lines to categorize: `expense_review_queue` where `is_uncategorized`, for the period asked about.
2. For each line, propose an account:
   - `suggested_account_name` is the account the payee's other categorized lines use most, and `suggestion_confidence` is the share of those lines on it. It is only set when the payee has at least 2 such lines and the share is at least 0.5.
   - If `suggestion_confidence` is at least 0.9 and the description fits, propose `suggested_account_name`.
   - Otherwise infer from `description` and `payee_name` (for example airlines and hotels to `Travel`, restaurants to `Meals & Entertainment`, software subscriptions to `Software & SaaS Tools`), and pick an account name that exists in `quickbooks_stage.accounts`.
3. Mark your confidence: **high** when the payee's history or the description is unambiguous, **unsure** otherwise. Flag every unsure line for a person, with the reason.
4. Also check `unusual_account` lines, coded away from an account used on at least 75% of the payee's 3 or more other lines, and `new_vendor` lines, the first charge from a payee: if the description shows the line is coded wrong, propose the fix; if the vendor legitimately sells more than one thing, say it is fine.
5. Return a table: date, payee, description, amount, current account, proposed account, confidence, reason. Do not apply the changes.

## Workflow: duplicates and anomalies

- **Possible duplicates**: `expense_review_queue` where `is_possible_duplicate`: the same payee (or the same description when there is no payee) and the same amount within 7 days of an earlier transaction. Look up `duplicate_of_transaction_id` in `quickbooks_stage.expense_lines.transaction_id` and compare descriptions; a match is usually a double charge or a bill entered twice.
- **Spikes**: `is_amount_spike` flags a transaction at least 1.75 times, and at least 500 more than, the average of the payee's previous three on the same account (`typical_amount`). Explain the spike from the description (a conference, a hire, usage growth) before calling it an error.
- **Window**: these flags only cover the last `review_window_days` (90 by default); uncategorized lines stay until fixed. Balance sheet lines such as card payoffs are in the queue too and can trip the spike or duplicate rules.

## Customizing

- Accounts are mapped in `assets/quickbooks_stage/account_mapping.csv`; `account_categories` rows with `is_mapped = false` use a default line. Propose new rows; the user applies them and reruns the pipeline.
- Placeholder account names used for `is_uncategorized` are in `assets/quickbooks_stage/expense_lines.sql`.
