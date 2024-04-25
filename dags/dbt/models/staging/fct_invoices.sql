


--- Dedup and standarized some columns
WITH std_invoices AS (
    SELECT  
        *
    FROM    
        {{ ref('intermediate_invoices') }}
)

-- Create FCT Table by date and organization_id granularity. Create Balance. 
-- Since amount and payment_amount have the same value, I supposed that both are in USD. 
-- (If not I would use case when to convert)

SELECT
    date_created,
    organization_id,
    SUM(amount_usd) AS total_amount_usd,
    SUM(payment_amount_usd) AS total_payment_amount_usd,
    SUM(amount_usd) - SUM(payment_amount_usd) AS daily_balance_usd,
    SUM(daily_balance_usd) OVER (PARTITION BY organization_id ORDER BY date_created ASC) AS acc_balance_usd
FROM
    std_invoices
WHERE status NOT IN ('canceled', 'failed','unpayable')
GROUP BY
    date_created, organization_id
ORDER BY organization_id, date_created DESC