-- Lignes de commande historiques (CSV) + lignes reçues en streaming via Kafka
WITH historique AS (
    SELECT id, order_id, product_id, quantity, unit_price
    FROM {{ source('raw_data', 'order_items') }}
),

flux AS (
    SELECT id, order_id, product_id, quantity, unit_price
    FROM {{ source('raw_data', 'order_items_stream') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at DESC) = 1
)

SELECT
id AS item_id,
order_id,
product_id,
quantity,
unit_price,
(quantity * unit_price) AS total_price
FROM (
    SELECT * FROM historique
    UNION ALL
    SELECT * FROM flux
)
