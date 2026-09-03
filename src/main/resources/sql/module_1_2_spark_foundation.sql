-- Module 1.2 - Spark Foundation
-- Spark SQL equivalent

-- High-value transactions
SELECT
    transaction_id,
    customer_id,
    city,
    amount
FROM transactions
WHERE amount >= 50000;


-- Total transaction amount by customer
SELECT
    customer_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS transaction_count
FROM transactions
GROUP BY customer_id
ORDER BY total_amount DESC;