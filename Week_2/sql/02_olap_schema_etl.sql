-- Bước 2: Star schema OLAP + ETL từ OLTP
-- Chạy: mysql -u root -p < 02_olap_schema_etl.sql
-- (không cần USE database trước vì script tự tạo và chuyển database)

CREATE DATABASE IF NOT EXISTS ecommerce_sales_dw
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE ecommerce_sales_dw;

-- 1. TẠO CÁC BẢNG DIMENSION + FACT

DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product_category;
DROP TABLE IF EXISTS dim_region;
DROP TABLE IF EXISTS dim_payment_method;

CREATE TABLE dim_date (
    date_id     INT PRIMARY KEY,          -- format YYYYMMDD
    full_date   DATE NOT NULL UNIQUE,
    year        INT NOT NULL,
    month       INT NOT NULL,
    quarter     INT NOT NULL,
    day_of_week VARCHAR(10) NOT NULL
);

CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY
);

CREATE TABLE dim_product_category (
    category_id   INT PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL
);

CREATE TABLE dim_region (
    region_id   INT PRIMARY KEY,
    region_name VARCHAR(50) NOT NULL
);

CREATE TABLE dim_payment_method (
    payment_method_id INT PRIMARY KEY,
    method_name        VARCHAR(50) NOT NULL
);

CREATE TABLE fact_orders (
    order_id           INT PRIMARY KEY,
    date_id             INT NOT NULL,
    customer_id         INT NOT NULL,
    category_id         INT NOT NULL,
    region_id           INT NOT NULL,
    payment_method_id   INT NOT NULL,
    quantity            INT NOT NULL,
    unit_price          DECIMAL(10, 2) NOT NULL,
    discount             DECIMAL(4, 2) NOT NULL,
    delivery_days        INT NOT NULL,
    customer_rating      DECIMAL(3, 1),
    revenue               DECIMAL(12, 2) NOT NULL,

    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (category_id) REFERENCES dim_product_category(category_id),
    FOREIGN KEY (region_id) REFERENCES dim_region(region_id),
    FOREIGN KEY (payment_method_id) REFERENCES dim_payment_method(payment_method_id)
);


-- 2. NẠP DIM_DATE
--    Chỉ nạp những ngày THỰC SỰ xuất hiện trong orders (không sinh khống
--    cả dải ngày liên tục) -- vừa tránh giới hạn cte_max_recursion_depth
--    của MySQL, vừa đúng nguyên tắc: dim_date không cần chứa ngày không
--    có giao dịch nào.

INSERT INTO dim_date (date_id, full_date, year, month, quarter, day_of_week)
SELECT DISTINCT
    CAST(DATE_FORMAT(order_date, '%Y%m%d') AS UNSIGNED) AS date_id,
    order_date AS full_date,
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    QUARTER(order_date) AS quarter,
    DAYNAME(order_date) AS day_of_week
FROM ecommerce_sales_db.orders;


-- 3. NẠP CÁC DIMENSION CÒN LẠI (copy trực tiếp từ OLTP)

INSERT INTO dim_customer (customer_id)
SELECT customer_id FROM ecommerce_sales_db.customers;

INSERT INTO dim_product_category (category_id, category_name)
SELECT category_id, category_name FROM ecommerce_sales_db.product_categories;

INSERT INTO dim_region (region_id, region_name)
SELECT region_id, region_name FROM ecommerce_sales_db.regions;

INSERT INTO dim_payment_method (payment_method_id, method_name)
SELECT payment_method_id, method_name FROM ecommerce_sales_db.payment_methods;


-- 4. NẠP FACT_ORDERS (join OLTP orders + customers để lấy region_id)

INSERT INTO fact_orders (
    order_id, date_id, customer_id, category_id, region_id, payment_method_id,
    quantity, unit_price, discount, delivery_days, customer_rating, revenue
)
SELECT
    o.order_id,
    CAST(DATE_FORMAT(o.order_date, '%Y%m%d') AS UNSIGNED) AS date_id,
    o.customer_id,
    o.category_id,
    c.region_id,
    o.payment_method_id,
    o.quantity,
    o.unit_price,
    o.discount,
    o.delivery_days,
    o.customer_rating,
    o.revenue
FROM ecommerce_sales_db.orders o
JOIN ecommerce_sales_db.customers c ON c.customer_id = o.customer_id;


-- 5. KIỂM TRA SAU ETL

SELECT 'fact_orders' AS table_name, COUNT(*) AS row_count FROM fact_orders
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product_category', COUNT(*) FROM dim_product_category
UNION ALL
SELECT 'dim_region', COUNT(*) FROM dim_region
UNION ALL
SELECT 'dim_payment_method', COUNT(*) FROM dim_payment_method;

-- Số dòng fact_orders phải bằng số dòng orders bên OLTP


-- 6. VÍ DỤ TRUY VẤN PHÂN TÍCH TRÊN OLAP (so sánh độ đơn giản với OLTP)

-- Doanh thu theo category, theo quý -- chỉ cần JOIN 2 bảng, không cần join sâu như OLTP
SELECT
    dc.category_name,
    dd.year,
    dd.quarter,
    SUM(f.revenue) AS total_revenue
FROM fact_orders f
JOIN dim_product_category dc ON dc.category_id = f.category_id
JOIN dim_date dd ON dd.date_id = f.date_id
GROUP BY dc.category_name, dd.year, dd.quarter
ORDER BY dd.year, dd.quarter, total_revenue DESC;