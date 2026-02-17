WITH monthly_calendar AS (

    SELECT month
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_TRUNC((SELECT MIN(purchase_date) FROM {{ ref('sales_ecom') }}), MONTH),
            DATE_TRUNC((SELECT MAX(purchase_date) FROM {{ ref('sales_ecom') }}), MONTH),
            INTERVAL 1 MONTH
        )
    ) AS month

),

last_purchase_per_client AS (

    SELECT
        client_id,
        MAX(purchase_date) AS last_purchase_date
    FROM {{ ref('sales_ecom') }}
    GROUP BY client_id

),

-- Base ativa = clientes com compra nos últimos 90 dias antes do mês
active_base AS (

    SELECT
        m.month,
        COUNT(DISTINCT s.client_id) AS active_base
    FROM monthly_calendar m
    JOIN {{ ref('sales_ecom') }} s
        ON s.purchase_date BETWEEN DATE_SUB(m.month, INTERVAL 90 DAY)
                               AND DATE_SUB(m.month, INTERVAL 1 DAY)
    GROUP BY m.month

),

-- churn_date = data em que completa 90 dias sem compra
customer_churn_dates AS (

    SELECT
        client_id,
        DATE_ADD(last_purchase_date, INTERVAL 90 DAY) AS churn_date
    FROM last_purchase_per_client

),

monthly_churn AS (

    SELECT
        DATE_TRUNC(churn_date, MONTH) AS month,
        COUNT(client_id) AS churned_customers
    FROM customer_churn_dates
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