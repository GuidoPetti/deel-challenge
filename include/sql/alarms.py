# FOR TESTING PURPOSES WE WILL CHECK 20 DAYS BEFORE, SO WE CAN GET SOME VALUES

sql_query_alarm = """

WITH set_diff_blc as (
    SELECT
        *,
        LAG(acc_balance_usd) OVER (PARTITION BY organization_id ORDER BY date_created ASC) AS previous_balance,
        DIV0(acc_balance_usd,previous_balance) - 1 AS perc_balance
    FROM
        dev_deel.staging.fct_invoices
    ORDER BY organization_id, date_created
)


SELECT 
    organization_id
FROM
    set_diff_blc WHERE date_created > DATEADD(DAY,-3,'{{ yesterday_ds_nodash }}')

"""
