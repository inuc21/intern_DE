-- Bước 1: Chuẩn hóa OLTP
-- Chạy: mysql -u root -p ecommerce_sales_db < 01_oltp_schema_migrate.sql

USE ecommerce_sales_db;

-- 0. Đổi tên bảng orders phẳng (Week 1) thành staging table
--    Giữ nguyên dữ liệu gốc để migrate, tránh trùng tên với bảng orders mới
DROP TABLE IF EXISTS stg_orders_flat;
RENAME TABLE orders TO stg_orders_flat;

-- 1. TẠO CÁC BẢNG CHUẨN HÓA (OLTP)

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS regions;
DROP TABLE IF EXISTS product_categories;
DROP TABLE IF EXISTS payment_methods;

CREATE TABLE regions (
    region_id   INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE product_categories (
    category_id   INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE payment_methods (
    payment_method_id INT AUTO_INCREMENT PRIMARY KEY,
    method_name        VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    region_id   INT NOT NULL,
    FOREIGN KEY (region_id) REFERENCES regions(region_id)
);

CREATE TABLE orders (
    order_id           INT PRIMARY KEY,
    order_date         DATE NOT NULL,
    customer_id        INT NOT NULL,
    category_id        INT NOT NULL,
    payment_method_id  INT NOT NULL,
    quantity           INT NOT NULL CHECK (quantity > 0),
    unit_price         DECIMAL(10, 2) NOT NULL,
    discount            DECIMAL(4, 2) NOT NULL,
    delivery_days       INT NOT NULL,
    customer_rating     DECIMAL(3, 1),
    revenue              DECIMAL(12, 2) NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (category_id) REFERENCES product_categories(category_id),
    FOREIGN KEY (payment_method_id) REFERENCES payment_methods(payment_method_id)
);


-- 2. MIGRATE DỮ LIỆU TỪ stg_orders_flat SANG CÁC BẢNG CHUẨN HÓA

-- 2.1 Nạp dimension: regions
INSERT INTO regions (region_name)
SELECT DISTINCT region FROM stg_orders_flat;

-- 2.2 Nạp dimension: product_categories
INSERT INTO product_categories (category_name)
SELECT DISTINCT product_category FROM stg_orders_flat;

-- 2.3 Nạp dimension: payment_methods
INSERT INTO payment_methods (method_name)
SELECT DISTINCT payment_method FROM stg_orders_flat;

-- 2.4 Nạp customers
-- Giả định: 1 customer_id gắn với 1 region duy nhất -> lấy region xuất hiện đầu tiên của mỗi customer
INSERT INTO customers (customer_id, region_id)
SELECT
    f.customer_id,
    r.region_id
FROM (
    SELECT customer_id, MIN(region) AS region
    FROM stg_orders_flat
    GROUP BY customer_id
) f
JOIN regions r ON r.region_name = f.region;

-- 2.5 Nạp orders (nối các khóa ngoại tương ứng)
INSERT INTO orders (
    order_id, order_date, customer_id, category_id, payment_method_id,
    quantity, unit_price, discount, delivery_days, customer_rating, revenue
)
SELECT
    f.order_id,
    f.order_date,
    f.customer_id,
    pc.category_id,
    pm.payment_method_id,
    f.quantity,
    f.unit_price,
    f.discount,
    f.delivery_days,
    f.customer_rating,
    f.revenue
FROM stg_orders_flat f
JOIN product_categories pc ON pc.category_name = f.product_category
JOIN payment_methods pm ON pm.method_name = f.payment_method;


-- 3. KIỂM TRA SAU MIGRATE

SELECT 'stg_orders_flat' AS table_name, COUNT(*) AS row_count FROM stg_orders_flat
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'regions', COUNT(*) FROM regions
UNION ALL
SELECT 'product_categories', COUNT(*) FROM product_categories
UNION ALL
SELECT 'payment_methods', COUNT(*) FROM payment_methods;

-- Số dòng orders và stg_orders_flat phải BẰNG NHAU, nếu lệch nghĩa là có dòng bị mất khi JOIN
-- (thường do category/payment_method có khoảng trắng/chữ hoa-thường không khớp)