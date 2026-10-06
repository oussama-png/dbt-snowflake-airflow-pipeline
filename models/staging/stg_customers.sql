SELECT
id AS customer_id,
name AS customer_name ,
email,
country
FROM {{ source('raw_data', 'customers') }}

--from finance_db.raw.customers the static form but in dbt we use the form below