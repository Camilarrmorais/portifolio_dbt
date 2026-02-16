WITH first_purchase AS (
    SELECT 
        client_id,
        MIN(purchase_date) AS first_purchase_date,
        FORMAT_DATE('%Y-%m', DATE_TRUNC(MIN(purchase_date), MONTH)) AS cohort_month
    FROM {{ref("sales_ecom")}}
    GROUP BY client_id
),

customer_activity AS (
    SELECT
        s.client_id,
        f.cohort_month,
        FORMAT_DATE('%Y-%m', DATE_TRUNC(s.purchase_date, MONTH)) AS activity_month,
        DATE_DIFF(
            DATE_TRUNC(s.purchase_date, MONTH),
            DATE_TRUNC(f.first_purchase_date, MONTH),
            MONTH
        ) AS month_number
    FROM {{ref("sales_ecom")}} s
    JOIN first_purchase f
        ON s.client_id = f.client_id
)

SELECT
    cohort_month,
    month_number,
    COUNT(DISTINCT client_id) AS active_customers
FROM customer_activity
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number