# Week 2 - Kiến trúc dữ liệu (OLTP/OLAP, ETL/ELT, CAP, ACID/BASE)

## Mục tiêu

Hiểu và áp dụng các khái niệm kiến trúc dữ liệu cốt lõi: OLTP vs OLAP, ETL vs ELT,
CAP theorem, ACID vs BASE. Thực hành bằng cách chuyển đổi dữ liệu từ bảng phẳng
(Week 1) sang schema OLTP chuẩn hóa và star schema OLAP.

## Đã học

- **OLTP vs OLAP**: OLTP tối ưu cho giao dịch (chuẩn hóa, tránh trùng lặp), OLAP tối
  ưu cho phân tích (denormalized, star/snowflake schema, ít bước JOIN hơn khi truy vấn
  tổng hợp).
- **ETL vs ELT**: ETL biến đổi dữ liệu trước khi load (phù hợp với quy mô dữ liệu nhỏ,
  MySQL đủ khả năng transform trong lúc insert); ELT tải thô trước, biến đổi sau
  (thường dùng cho big data: Tuần 3 với Spark).
- **CAP theorem**: hệ phân tán chỉ đảm bảo tối đa 2/3 giữa Consistency, Availability,
  Partition tolerance. Chưa áp dụng trực tiếp được ở tuần này vì hệ thống chạy trên
  MySQL single-node, không có phân vùng mạng giữa các node.
- **ACID vs BASE**: MySQL là hệ quản trị CSDL quan hệ tuân theo ACID, đảm bảo
  tính nhất quán và độ tin cậy của dữ liệu. Trong quá trình migrate dữ liệu,
  các ràng buộc khóa chính, khóa ngoại và kiểu dữ liệu giúp ngăn dữ liệu không
  hợp lệ được ghi vào hệ thống.

## Đã thực hành

- **Mô tả bài toán**: từ bảng `orders` phẳng (Week 1, chứa toàn bộ thông tin category/
  region/payment_method lặp lại ở mỗi dòng), thiết kế lại theo 2 hướng kiến trúc khác
  nhau phục vụ 2 mục đích khác nhau.
- **Cách giải quyết**:
  - Chuẩn hóa OLTP: tách thành 5 bảng có quan hệ (`regions`, `product_categories`,
    `payment_methods`, `customers`, `orders`) trong database `ecommerce_sales_db`.
  - Thiết kế OLAP: xây dựng star schema với `fact_orders` + 5 dimension
    (`dim_date`, `dim_customer`, `dim_product_category`, `dim_region`,
    `dim_payment_method`) trong database `ecommerce_sales_dw`.
  - Viết SQL migrate dữ liệu từ bảng phẳng sang OLTP, sau đó ETL từ OLTP sang OLAP.
- **Kết quả**: ETL chạy thành công, `fact_orders` = 2,618 dòng, khớp chính xác với
  `orders` bên OLTP. Chi tiết đầy đủ và các bảng số liệu ở
  [`docs/architecture_notes.md`](./docs/architecture_notes.md), sơ đồ ERD ở thư mục
  [`diagrams/`](./diagrams/).

## Khó khăn gặp phải

- Dùng `WITH RECURSIVE` để tự sinh dải ngày liên tục cho `dim_date` bị lỗi
  `ERROR 3636 (HY000): Recursive query aborted after 1001 iterations` vì dữ liệu
  trải dài 4,997 ngày (2022-2035), vượt giới hạn mặc định `cte_max_recursion_depth = 1000`
  của MySQL. Xử lý bằng cách đổi sang `SELECT DISTINCT order_date FROM orders` — chỉ
  nạp những ngày thực sự có giao dịch, vừa tránh giới hạn kỹ thuật vừa đúng nguyên tắc
  thiết kế hơn. Điều này cũng giúp giảm kích thước dimension và phản ánh đúng những ngày
  thực sự phát sinh giao dịch.
- `MAX(order_date)` ra năm 2035 ban đầu nghi là outlier/lỗi dữ liệu. Kiểm tra bằng
  `GROUP BY YEAR(order_date)` thấy đơn hàng phân bố đều ~185-200 đơn/năm suốt 14 năm,
  không có cụm bất thường → kết luận đây là đặc điểm sinh dữ liệu synthetic có chủ đích,
  không phải lỗi cần clean lại.
- `payment_methods` chỉ có 2 giá trị (`Wallet`, `Card`) thay vì 3 như danh sách
  `VALID_PAYMENTS` phòng thủ ở Week 1 — xác nhận là do dataset gốc không có `COD`,
  không phải lỗi migrate.

## Việc cần làm tiếp

- Sang Tuần 3: xử lý dữ liệu lớn bằng Apache Spark, làm quen HDFS và định dạng
  Parquet/ORC.
- Cân nhắc lại giả định "1 customer thuộc 1 region cố định" nếu dataset thực tế ở
  công ty có khách hàng đặt hàng từ nhiều vùng khác nhau — khi đó chuyển `region`
  thành thuộc tính của `orders` thay vì `customers`.
