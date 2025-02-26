CREATE TABLE CLYMAPPO.sgv_dtac_account_balances (
    account_id NUMERIC(22,2),
    customer_no VARCHAR(300),
    ar_balance NUMERIC(22,2),
    open_amount NUMERIC(22,2),
    unapplied_amount NUMERIC(22,2),
    dispute_amount NUMERIC(22,2),
    pending_cr_amount NUMERIC(22,2),
    last_trx_date TIMESTAMP,
    balance_update_date TIMESTAMP,
    last_payment_date TIMESTAMP,
    load_date TIMESTAMP,
    create_at TIMESTAMP,
    update_at TIMESTAMP,
    transaction_date DATE,
    partition_date DATE,
    partition_month VARCHAR(10),
    processed_date DATE
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;
