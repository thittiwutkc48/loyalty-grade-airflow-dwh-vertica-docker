CREATE TABLE CLYMAPPO.sgv_dtac_invoices (
    invoice_id NUMERIC(22,2),
    invoice_no VARCHAR(400),
    parent_invoice_id NUMERIC(22,2),
    transaction_type VARCHAR(200),
    account_id NUMERIC(22,2),
    customer_no VARCHAR(300),
    product_id VARCHAR(100),
    invoice_amount NUMERIC(22,2),
    tax_amount NUMERIC(22,2),
    invoice_date TIMESTAMP,
    due_date DATE,
    invoice_status VARCHAR(100),
    bill_cycle_id VARCHAR(100),
    bill_month VARCHAR(10),
    last_update_date TIMESTAMP,
    entry_date TIMESTAMP,
    load_date TIMESTAMP,
    transaction_date DATE,
    partition_date DATE,
    partition_month VARCHAR(10),
    processed_date DATE
)
UNSEGMENTED ALL NODES
PARTITION BY partition_date;
