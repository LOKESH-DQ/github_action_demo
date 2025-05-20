/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below_details
*/

WITH customer_data AS (
SELECT
o.CUSTOMER_ID,
o.ORDER_DATE,
o.STATUS,
c.FIRST_NAME,
c.LAST_NAME,
ROW_NUMBER() OVER (ORDER BY o.ORDER_DATE DESC) AS row_num
FROM
DQLABS_QA.CUSTOMERAI.stg_orders o
JOIN
DQLABS_QA.CUSTOMERAI.stg_customer c
ON
o.CUSTOMER_ID = c.CUSTOMER_ID
)

SELECT
new_column,
CUSTOMER_ID,
ORDER_DATE,
STATUS,
FIRST_NAME,
LAST_NAME
FROM
customer_data


-- Incrementally load 5 new rows per run
WHERE row_num > (SELECT COUNT(*) FROM DQLABS_QA.CUSTOMERAI.customerai_dbt_incr)
AND row_num <= ((SELECT COUNT(*) FROM DQLABS_QA.CUSTOMERAI.customerai_dbt_incr) + 5)

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
