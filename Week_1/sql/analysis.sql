-- Bước 3: Phân tích dữ liệu + tối ưu index
USE ecommerce_sales_db;

-- 1. TRUY VẤN PHÂN TÍCH CƠ BẢN

-- Doanh thu theo từng category
SELECT product_category, SUM(revenue) AS total_revenue, COUNT(*) AS order_count
FROM orders
GROUP BY product_category
ORDER BY total_revenue DESC;

-- Doanh thu theo vùng miền, theo tháng
SELECT
    region,
    DATE_FORMAT(order_date, '%Y-%m') AS month,
    SUM(revenue) AS total_revenue
FROM orders
GROUP BY region, month
ORDER BY month, region;

-- Top 10 khách hàng chi tiêu nhiều nhất
SELECT customer_id, SUM(revenue) AS total_spent, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- Phương thức thanh toán nào có rating trung bình cao nhất
SELECT payment_method, AVG(customer_rating) AS avg_rating, COUNT(*) AS order_count
FROM orders
GROUP BY payment_method
ORDER BY avg_rating DESC;

-- Mối liên hệ giữa delivery_days và customer_rating
SELECT
    CASE
        WHEN delivery_days <= 3 THEN '1-3 ngày'
        WHEN delivery_days <= 7 THEN '4-7 ngày'
        ELSE '8+ ngày'
    END AS delivery_bucket,
    AVG(customer_rating) AS avg_rating,
    COUNT(*) AS order_count
FROM orders
GROUP BY delivery_bucket
ORDER BY delivery_bucket;


-- 2. XEM EXECUTION PLAN TRƯỚC KHI CÓ INDEX

EXPLAIN
SELECT customer_id, SUM(revenue) AS total_spent
FROM orders
WHERE region = 'South' AND order_date >= '2022-06-01'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- Ghi lại kết quả EXPLAIN vào results/explain_before_index.txt
-- Chú ý các cột: type (ALL = full table scan, xấu), rows (số dòng phải quét), key (NULL = không dùng index)


-- 3. TẠO INDEX VÀ SO SÁNH

-- Index đơn cho các cột hay dùng ở WHERE/GROUP BY riêng lẻ
CREATE INDEX idx_orders_region ON orders(region);
CREATE INDEX idx_orders_customer ON orders(customer_id);

-- Composite index cho truy vấn lọc theo region + order_date (đúng thứ tự dùng trong WHERE)
CREATE INDEX idx_orders_region_date ON orders(region, order_date);

-- Chạy lại EXPLAIN với cùng câu query ở trên để so sánh
EXPLAIN
SELECT customer_id, SUM(revenue) AS total_spent
FROM orders
WHERE region = 'South' AND order_date >= '2022-06-01'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- Ghi lại kết quả vào results/explain_after_index.txt
-- So sánh: type có chuyển từ ALL sang range/ref không? rows quét có giảm không? key có dùng idx_orders_region_date không?


-- 4. KIỂM TRA INDEX ĐÃ TẠO

SHOW INDEX FROM orders;