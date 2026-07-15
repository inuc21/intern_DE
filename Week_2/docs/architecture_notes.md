# Architecture Notes - Week 2

## 1. Bối cảnh

Dữ liệu đầu vào là bảng `orders` phẳng (denormalized) từ Week 1, chứa toàn bộ thông tin
đơn hàng trong 1 bảng duy nhất (customer, category, region, payment method đều là các
cột text lặp lại ở mỗi dòng). Mục tiêu Tuần 2: tách bảng này theo 2 hướng kiến trúc khác
nhau, phục vụ 2 mục đích khác nhau — OLTP cho giao dịch, OLAP cho phân tích.

## 2. Thiết kế OLTP (database: `ecommerce_sales_db`)

### Quyết định chuẩn hóa

Bảng phẳng ban đầu có 3 cột lặp lại nhiều lần trên các dòng khác nhau: `product_category`,
`region`, `payment_method`. Tách thành các bảng lookup riêng để:

- Tránh trùng lặp dữ liệu (chỉ 3 category, 4 region, 2 payment method nhưng lặp lại trên
  toàn bộ 2,618 dòng).
- Đảm bảo tính nhất quán — nếu sau này cần đổi tên 1 category, chỉ sửa 1 dòng thay vì
  hàng nghìn dòng.
- Cho phép mở rộng thuộc tính của từng entity sau này (ví dụ thêm mô tả cho category)
  mà không phải sửa bảng `orders`.

### Schema cuối cùng

```
regions (region_id PK, region_name)
product_categories (category_id PK, category_name)
payment_methods (payment_method_id PK, method_name)
customers (customer_id PK, region_id FK)
orders (order_id PK, order_date, customer_id FK, category_id FK,
        payment_method_id FK, quantity, unit_price, discount,
        delivery_days, customer_rating, revenue)
```

### Giả định đã đưa ra

`region` trong dataset gốc được coi là thuộc tính cố định của `customer` (1 khách hàng
thuộc đúng 1 vùng), lấy theo vùng xuất hiện đầu tiên của customer_id đó trong dữ liệu gốc.
Đây là giả định đơn giản hóa cho bài tập — trong hệ thống thực tế, `region` nhiều khả năng
nên gắn với `orders` (vùng giao hàng của từng đơn) vì một khách hàng có thể đặt hàng giao
đến nhiều nơi khác nhau.

### Kết quả migrate

| Bảng               | Số dòng |
| ------------------ | ------- |
| orders             | 2,618   |
| customers          | 913     |
| regions            | 4       |
| product_categories | 3       |
| payment_methods    | 2       |

Số dòng `orders` sau chuẩn hóa khớp chính xác với `stg_orders_flat` gốc — xác nhận
không có dòng nào bị mất trong quá trình JOIN migrate.

## 3. Thiết kế OLAP (database: `ecommerce_sales_dw`)

### Vì sao chọn star schema

Star schema (1 fact table + các dimension) được chọn thay vì snowflake vì dataset có
quy mô nhỏ và các dimension không có nhiều cấp phân cấp (category không có sub-category,
region không chia nhỏ hơn). Star schema đơn giản hơn, JOIN ít tầng hơn, phù hợp cho
mục đích phân tích nhanh.

### Schema

```
fact_orders (order_id, date_id FK, customer_id FK, category_id FK,
             region_id FK, payment_method_id FK, quantity, unit_price,
             discount, delivery_days, customer_rating, revenue)

dim_date (date_id, full_date, year, month, quarter, day_of_week)
dim_customer (customer_id)
dim_product_category (category_id, category_name)
dim_region (region_id, region_name)
dim_payment_method (payment_method_id, method_name)
```

### So sánh với OLTP

Cùng một câu hỏi phân tích ("doanh thu theo category, theo quý"):

- Trên OLTP: phải JOIN `orders` → `product_categories`, tính toán trực tiếp trên dữ liệu
  giao dịch chi tiết.
- Trên OLAP: JOIN `fact_orders` → `dim_product_category` → `dim_date`, cấu trúc phẳng hơn,
  không cần đi qua bảng `customers` trung gian để lấy region như bên OLTP.

Sự khác biệt rõ nhất khi hệ thống có nhiều bảng giao dịch hơn (không chỉ orders mà còn
returns, payments, shipments...) — OLTP sẽ cần join qua rất nhiều bảng để trả lời 1 câu
hỏi phân tích, trong khi OLAP luôn chỉ cách fact table 1 bước JOIN tới bất kỳ dimension nào.

## 4. ETL áp dụng ở đâu

Toàn bộ pipeline Tuần 1-2 đi theo hướng **ETL** (Extract → Transform → Load), không phải
ELT:

- Extract: đọc CSV gốc.
- Transform: clean dữ liệu (Week 1), sau đó chuẩn hóa thành OLTP, rồi biến đổi tiếp
  thành OLAP (Week 2) — dữ liệu được biến đổi ở mỗi bước trước khi lưu vào bảng đích.
- Load: ghi vào MySQL ở từng giai đoạn.

Lý do phù hợp ELT thay vì ELT ở quy mô này: dữ liệu nhỏ (2,618 dòng), MySQL đủ khả năng
xử lý transform ngay trong quá trình insert bằng SQL JOIN, không cần tải thô rồi mới xử lý
như cách tiếp cận ELT thường dùng cho big data (Tuần 3 với Spark).

## 5. Liên hệ CAP theorem và ACID/BASE

Toàn bộ hệ thống hiện tại chạy trên 1 instance MySQL single-node, nên CAP theorem chưa
thực sự "phát huy tác dụng" — không có phân vùng mạng (partition) giữa các node để phải
đánh đổi Consistency hay Availability. Khái niệm này sẽ rõ ràng hơn khi triển khai HDFS/Kafka
ở Tuần 3-4, là các hệ phân tán thật sự.

MySQL đảm bảo **ACID** cho toàn bộ các bước INSERT trong quá trình migrate — mỗi câu lệnh
`INSERT INTO ... SELECT` chạy trong 1 transaction, nếu lỗi giữa chừng sẽ rollback toàn bộ,
không để dữ liệu ở trạng thái nửa vời. Điều này quan trọng với OLTP vì tính đúng đắn của
giao dịch phải được đảm bảo tuyệt đối.

## 6. Vấn đề gặp phải và cách xử lý

### Recursive CTE vượt giới hạn `cte_max_recursion_depth`

Ban đầu dùng `WITH RECURSIVE` để tự sinh dải ngày liên tục cho `dim_date`. Dữ liệu trải
dài 4,997 ngày (2022-01-01 đến 2035-09-07) vượt giới hạn mặc định của MySQL
(`cte_max_recursion_depth = 1000`), gây lỗi `ERROR 3636`.

Cách xử lý: thay vì sinh dải ngày liên tục rồi tăng giới hạn recursion, chuyển sang
`SELECT DISTINCT order_date FROM orders` để chỉ nạp đúng những ngày thực sự có giao dịch.
Cách này vừa tránh được giới hạn kỹ thuật, vừa đúng nguyên tắc thiết kế hơn — `dim_date`
không cần chứa những ngày không có dữ liệu nào tham chiếu tới.

### Kiểm tra outlier date trước khi kết luận

`MAX(order_date) = 2035-09-07` ban đầu trông giống dữ liệu lỗi. Kiểm tra bằng
`GROUP BY YEAR(order_date)` cho thấy đơn hàng phân bố đều ~185-200 đơn mỗi năm suốt
14 năm (2022-2035), không có cụm bất thường — kết luận đây là đặc điểm sinh dữ liệu
synthetic có chủ đích, không phải lỗi cần clean lại ở Week 1.

Bài học: không nên vội sửa dữ liệu "trông có vẻ bất thường" chỉ dựa vào 1 giá trị min/max;
cần kiểm tra phân bố (distribution) trước khi kết luận đó là outlier hay đặc điểm hợp lệ
của dataset.

## 7. Kết quả cuối cùng

| Bảng OLAP            | Số dòng |
| -------------------- | ------- |
| fact_orders          | 2,618   |
| dim_date             | 2,618   |
| dim_customer         | 913     |
| dim_product_category | 3       |
| dim_region           | 4       |
| dim_payment_method   | 2       |

`fact_orders` khớp chính xác với `orders` bên OLTP (2,618 dòng) — xác nhận ETL không làm
mất dữ liệu.
