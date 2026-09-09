-- Operational payments database: the system of record for card authorizations.
--
-- This mirrors what a payments processor would already own. The pipeline never
-- creates this table; the Python seed asset writes into it and the ingestr CDC
-- asset replicates it into ClickHouse.

CREATE SCHEMA IF NOT EXISTS payments;

CREATE TABLE IF NOT EXISTS payments.transactions (
    transaction_id    bigint PRIMARY KEY,
    created_at        timestamptz   NOT NULL,          -- authorization time (rollup event time)
    updated_at        timestamptz   NOT NULL,          -- CDC version cursor; bumps on status change
    merchant_id       bigint        NOT NULL,
    card_id           bigint        NOT NULL,          -- tokenized card reference
    amount_cents      bigint        NOT NULL,          -- amount in minor units; see README on why not numeric
    currency          text          NOT NULL DEFAULT 'USD',
    status            text          NOT NULL,          -- approved | declined | refunded | chargeback
    decline_reason    text,                            -- insufficient_funds | do_not_honor | fraud_suspected | expired_card | NULL
    card_network      text          NOT NULL,          -- visa | mastercard | amex | discover
    merchant_category text          NOT NULL,          -- grocery | travel | digital_goods | gaming | ...
    country           text          NOT NULL,          -- ISO-2 country of the transaction
    is_fraud          boolean       NOT NULL DEFAULT false,
    auth_latency_ms   integer       NOT NULL           -- authorization latency (P95 KPI)
);

CREATE INDEX IF NOT EXISTS transactions_updated_at_idx
    ON payments.transactions (updated_at);

-- Logical replication prerequisites for Bruin CDC.
--
-- REPLICA IDENTITY FULL makes UPDATE and DELETE events carry the complete old
-- row, which is what lets a status restatement (approved -> chargeback) be
-- replicated rather than dropped.
ALTER TABLE payments.transactions REPLICA IDENTITY FULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'bruin_payments_pub'
    ) THEN
        CREATE PUBLICATION bruin_payments_pub FOR TABLE payments.transactions;
    END IF;
END
$$;
