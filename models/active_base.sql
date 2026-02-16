WITH monthly_calendar AS (

    -- gera meses entre primeira e última venda
    SELECT month
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT DATE_TRUNC(MIN(purchase_date), MONTH) FROM {{ref("sales_ecom")}}),
            (SELECT DATE_TRUNC(MAX(purchase_date), MONTH) FROM {{ref("sales_ecom")}}),
            INTERVAL 1 MONTH
        )
    ) AS month

),

last_purchase AS (

    SELECT
        client_id,
        MAX(purchase_date) AS last_purchase_date
    FROM {{ref("sales_ecom")}}
    GROUP BY client_id

),

customer_status AS (

    SELECT
        client_id,
        last_purchase_date,
        DATE_ADD(last_purchase_date, INTERVAL 90 DAY) AS churn_date
    FROM last_purchase

),

active_base AS (

    SELECT
        m.month,
        COUNT(DISTINCT s.client_id) AS active_base
    FROM monthly_calendar m
    JOIN {{ref("sales_ecom")}} s
        ON s.purchase_date < m.month
    GROUP BY m.month

),

monthly_churn AS (

    SELECT
        DATE_TRUNC(churn_date, MONTH) AS month,
        COUNT(client_id) AS churned_customers
    FROM customer_status
    WHERE churn_date <= (SELECT MAX(purchase_date) FROM {{ref("sales_ecom")}})
    GROUP BY month

)

SELECT
    m.month,
    COALESCE(a.active_base, 0) AS active_base,
    COALESCE(c.churned_customers, 0) AS churned_customers,
    SAFE_DIVIDE(
        COALESCE(c.churned_customers, 0),
        COALESCE(a.active_base, 0)
    ) AS churn_rate
FROM monthly_calendar m
LEFT JOIN active_base a
    ON m.month = a.month
LEFT JOIN monthly_churn c
    ON m.month = c.month
ORDER BY m.month