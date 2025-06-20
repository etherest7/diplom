CREATE DATABASE IF NOT EXISTS data_mart;

CREATE TABLE IF NOT EXISTS data_mart.sales_mart
(
    invoice_id String,
    datetime DateTime,
    city_name String,
    branch String,
    product_line String,
    product_price Decimal(10, 2),
    customer_gender String,
    customer_type String,
    payment_method String,
    quantity Int32,
    tax Decimal(10, 2),
    total Decimal(10, 2),
    cogs Decimal(10, 2),
    margin_percentage Decimal(10, 2),
    gross_income Decimal(10, 2),
    rating Decimal(3, 1)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (datetime, invoice_id);
