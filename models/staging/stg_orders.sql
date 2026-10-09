-- Commandes historiques (CSV) + commandes reçues en streaming via Kafka
WITH historique AS (
    SELECT id, customer_id, order_date, status, 'batch' AS source_systeme
    FROM {{ source('raw_data', 'orders') }}
),

flux AS (
    -- Garantie at-least-once : un lot peut être inséré deux fois, on garde la dernière version de chaque commande
    SELECT id, customer_id, order_date, status, 'kafka' AS source_systeme
    FROM {{ source('raw_data', 'orders_stream') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at DESC) = 1
)

SELECT
id AS order_id,
customer_id,
order_date,
status AS order_status,
source_systeme
FROM (
    SELECT * FROM historique
    UNION ALL
    SELECT * FROM flux
)
