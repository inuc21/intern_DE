Intern: HÀ VIỆT HOÀNG

Team: Platform Adtech

# Week_1 - Nền tảng cơ bản (Python / SQL / Linux / MySQL)

## Mục tiêu

- Thực hành xử lý dữ liệu bằng Python.
- Làm sạch dữ liệu từ file CSV.
- Thiết kế schema và lưu trữ dữ liệu trên MySQL.
- Viết truy vấn SQL phân tích dữ liệu.
- Tìm hiểu và áp dụng Index, EXPLAIN để tối ưu truy vấn.

---

## Dataset sử dụng

**E-Commerce Sales Analytics Dataset**

- Số dòng: 5000
- Số cột: 12

Các trường dữ liệu chính:

- order_id
- order_date
- customer_id
- product_category
- region
- quantity
- unit_price
- discount
- payment_method
- delivery_days
- customer_rating
- revenue

---

## Khám phá dữ liệu (Data Profiling)

### Tổng quan

- Dataset gồm 5000 bản ghi và 12 thuộc tính.
- Không phát hiện lỗi nghiêm trọng về cấu trúc dữ liệu.
- Công thức doanh thu được kiểm tra:

revenue = quantity × unit_price × (1 - discount)

Kết quả:

- 0/5000 bản ghi sai lệch công thức doanh thu.

### Kiểm tra chất lượng dữ liệu

Đã thực hiện:

- Kiểm tra missing values.
- Kiểm tra duplicate records.
- Kiểm tra giá trị âm.
- Kiểm tra kiểu dữ liệu.
- Kiểm tra tính hợp lệ của các trường categorical.

---

## Thiết kế Database

### Database

ecommerce_sales_db

### Bảng chính

orders

Các ràng buộc được áp dụng:

- Primary Key: order_id
- NOT NULL
- CHECK constraints:
  - quantity > 0
  - unit_price >= 0
  - discount BETWEEN 0 AND 1
  - customer_rating BETWEEN 1 AND 5

### Metadata

Bổ sung cột:

loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

để phục vụ tracking dữ liệu khi thực hiện ETL pipeline.

---

## Làm sạch dữ liệu (Data Cleaning)

Các bước thực hiện:

### 1. Chuẩn hóa dữ liệu ngày tháng

- Chuyển order_date sang kiểu datetime.

### 2. Xử lý dữ liệu thiếu

- Loại bỏ các bản ghi thiếu trường bắt buộc.
- Điền giá trị median cho:
  - customer_rating
  - delivery_days

- Điền "Unknown" cho payment_method.

### 3. Xử lý dữ liệu trùng lặp

- Loại bỏ các bản ghi trùng order_id.

### 4. Kiểm tra dữ liệu bất thường

- Loại bỏ các bản ghi có:
  - quantity <= 0
  - unit_price < 0
  - revenue < 0

### 5. Chuẩn hóa dữ liệu phân loại

- Chuẩn hóa format text.
- Kiểm tra danh mục hợp lệ:
  - Product Category
  - Region
  - Payment Method

### 6. Kiểm tra Business Rule

Kiểm tra công thức:

revenue = quantity × unit_price × (1 - discount)

Kết quả:

- Không phát hiện sai lệch đáng kể.

---

## Nạp dữ liệu vào MySQL

Pipeline:

CSV
→ Data Cleaning
→ Processed CSV
→ MySQL

Kỹ thuật sử dụng:

- mysql-connector-python
- executemany()
- ON DUPLICATE KEY UPDATE

Lợi ích:

- Có thể chạy pipeline nhiều lần mà không tạo dữ liệu trùng lặp.

---

## Phân tích dữ liệu bằng SQL

### Doanh thu theo danh mục sản phẩm

Phân tích tổng doanh thu và số lượng đơn hàng theo từng category.

### Doanh thu theo vùng và thời gian

Phân tích doanh thu theo tháng và khu vực.

### Top khách hàng chi tiêu nhiều nhất

Xác định nhóm khách hàng có tổng doanh thu cao nhất.

### Đánh giá phương thức thanh toán

So sánh customer_rating giữa các phương thức thanh toán.

### Tác động của thời gian giao hàng

Phân tích mối quan hệ giữa delivery_days và customer_rating.

---

## Tối ưu truy vấn

### Truy vấn được kiểm tra

Phân tích khách hàng theo:

- region
- order_date

### Index đã tạo

- idx_orders_region
- idx_orders_customer
- idx_orders_region_date

### EXPLAIN

Đã sử dụng EXPLAIN để kiểm tra execution plan trước và sau khi tạo index.

Nhận xét:

- Dataset hiện tại chỉ gồm 5000 bản ghi nên mức cải thiện chưa rõ rệt.
- Composite index (region, order_date) phù hợp với điều kiện lọc của truy vấn.
- Trên tập dữ liệu lớn hơn, index sẽ giúp giảm số lượng bản ghi cần quét và cải thiện hiệu năng.

---

## Kiến thức học được

- Python Data Processing với Pandas.
- Data Cleaning và Data Validation.
- Thiết kế schema MySQL.
- SQL Aggregation và Analytics.
- Index và Execution Plan.
- Quy trình ETL cơ bản từ CSV vào Database.

---

## Khó khăn gặp phải

- Cấu hình môi trường Ubuntu (WSL2).
- Kết nối MySQL từ môi trường thực hành.
- Hiểu cách hoạt động của Index và EXPLAIN.
- Xử lý dữ liệu đầu vào theo các business rules.

---

## Kết quả đạt được

- Hoàn thành pipeline CSV → Clean Data → MySQL.
- Xây dựng schema và các truy vấn phân tích dữ liệu.
- Áp dụng Index và EXPLAIN để tối ưu truy vấn.
