
{{
    config(
        materialized='incremental',
        strategy= 'merge',
        unique_key=['invoice_id','transaction_id']
    )
}}

    SELECT
        ABS(invoice_id) AS invoice_id,
        ABS(parent_invoice_id) AS parent_invoice_id,
        ABS(transaction_id) AS transaction_id,
        ABS(organization_id) AS organization_id,
        ABS(type) AS type,
        status,
        currency,
        payment_currency,
        payment_method,
        amount,
        nvl(amount,0) AS amount_usd,
        payment_amount,
        nvl(payment_amount,0) AS payment_amount_usd,
        fx_rate,
        fx_rate_payment,
        created_at,
        to_date(created_at) AS date_created
    FROM
        {{ source('raw','invoices') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ABS(invoice_id),ABS(organization_id) ORDER BY created_at DESC) = 1

    