WITH invoices_summary AS(
    SELECT
            ABS(organization_id) as organization_id,
            ARRAY_AGG(DISTINCT payment_method) AS payment_methods,
            SUM(CASE WHEN status = 'awaiting_payment' THEN 1 ELSE 0 END) AS inv_awaiting_payment,
            SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS invoices_skipped,
            SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS invoices_processing,
            SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS invoices_refunded,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS invoices_failed,
            SUM(CASE WHEN status = 'unpayable' THEN 1 ELSE 0 END) AS invoices_unpayable,
            SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS invoices_cancelled,
            SUM(CASE WHEN status = 'credited' THEN 1 ELSE 0 END) AS invoices_credited,
            SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS invoices_paid,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS invoices_pending
    FROM
        {{ source('raw','invoices') }} 
    GROUP BY organization_id
),
balance_acc AS (

        SELECT  
            organization_id,
            acc_balance_usd
        FROM
            {{ ref('fct_invoices') }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY date_created desc) = 1
)


SELECT
    org.*,
    i.*
    EXCLUDE (organization_id),
    blc.acc_balance_usd
FROM
    {{ source('raw','organizations') }} org
LEFT JOIN
    invoices_summary i
ON 
    org.organization_id = i.organization_id
LEFT JOIN
    balance_acc blc
ON
    org.organization_id = blc.organization_id