CREATE SCHEMA IF NOT EXISTS diplom;

CREATE TABLE diplom.city (
  city_id SERIAL PRIMARY KEY,
  city_name VARCHAR(50) UNIQUE
);

CREATE TABLE diplom.branch (
  branch_id SERIAL PRIMARY KEY,
  city_id INT REFERENCES diplom.city(city_id),
  branch VARCHAR(5),
  UNIQUE (branch, city_id)
);

CREATE TABLE diplom.customer (
  customer_id SERIAL PRIMARY KEY,
  gender VARCHAR(10),
  type VARCHAR(10),
  UNIQUE (gender, type)
);

CREATE TABLE diplom.product (
  product_id SERIAL PRIMARY KEY,
  product_line VARCHAR(50),
  product_price NUMERIC,
  UNIQUE (product_line, product_price)
);

CREATE TABLE diplom.payment_method (
  payment_method_id SERIAL PRIMARY KEY,
  method_name VARCHAR(20) UNIQUE
);

CREATE TABLE diplom.sales (
  invoice_id varchar(20) PRIMARY KEY,
  product_id INT REFERENCES diplom.product(product_id),
  customer_id INT REFERENCES diplom.customer(customer_id),
  branch_id INT REFERENCES diplom.branch(branch_id),
  payment_method_id INT REFERENCES diplom.payment_method(payment_method_id),
  datetime TIMESTAMP NOT NULL,
  quantity INT,
  tax NUMERIC,
  total NUMERIC,
  cogs NUMERIC,  
  margin_percentage NUMERIC,
  gross_income NUMERIC,
  rating NUMERIC
);