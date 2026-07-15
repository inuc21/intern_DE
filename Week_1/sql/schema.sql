-- Schema cho dataset E-Commerce Sales Analytics

CREATE DATABASE IF NOT EXISTS ecommerce_sales_db
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE ecommerce_sales_db;

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id         INT PRIMARY KEY,
    order_date       DATE NOT NULL,
    customer_id      INT NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    region           VARCHAR(50) NOT NULL,
    quantity         INT NOT NULL CHECK (quantity > 0),
    unit_price       DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount         DECIMAL(4, 2) NOT NULL CHECK (discount BETWEEN 0 AND 1),
    payment_method   VARCHAR(20) NOT NULL,
    delivery_days    INT NOT NULL CHECK (delivery_days >= 0),
    customer_rating  DECIMAL(3, 1) CHECK (customer_rating BETWEEN 1 AND 5),
    revenue          DECIMAL(12, 2) NOT NULL,

    -- metadata phục vụ tracking pipeline (thói quen tốt cho production sau này)
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);