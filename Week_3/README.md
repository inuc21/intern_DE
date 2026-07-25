# Week 3 - Big Data: Batch Processing (Apache Spark)

## Mục tiêu

Làm quen xử lý dữ liệu lớn theo lô (batch) bằng Apache Spark, hiểu kiến trúc Spark
(Driver/Executor/Cluster Manager), và so sánh hiệu năng giữa các định dạng file
(CSV, Parquet, ORC) cùng kỹ thuật partitioning.

## Đã học

- **Kiến trúc Spark**: Driver điều phối, Executor thực thi task, Cluster Manager quản lý
  tài nguyên. Ở local mode (`master("local[*]")`), Driver và Executor chạy chung 1 máy,
  dùng toàn bộ CPU core để giả lập xử lý song song.
- **Lazy evaluation**: Spark không thực thi ngay khi gọi transformation (`.read()`,
  `.groupBy()`...), chỉ lên kế hoạch — thực sự chạy khi gặp action (`.count()`,
  `.collect()`, `.show()`).
- **DataFrame API vs Spark SQL**: hai cách viết khác nhau nhưng cùng biên dịch về 1
  execution plan phía sau; Spark SQL có cú pháp gần giống hệt SQL chuẩn đã dùng ở Week 1-2.
- **Columnar storage (Parquet/ORC)**: lưu dữ liệu theo cột thay vì theo dòng như CSV, cho
  phép chỉ đọc đúng cột cần dùng khi query, giảm cả dung lượng lẫn thời gian đọc đáng kể.
- **Partitioning & partition pruning**: chia dữ liệu thành thư mục con theo 1 cột hay
  dùng để filter (ví dụ theo năm), giúp Spark bỏ qua hoàn toàn các phần dữ liệu không
  liên quan ngay từ bước đọc file.

## Đã thực hành

- **Setup môi trường**: cài Java 17, PySpark 3.5.1 trong venv riêng (`venv_spark`, Python
  3.12) để tránh xung đột với venv chính (Python 3.14) đang dùng cho Week 1-2.
- **Sinh dataset 1.2GB+**: viết script Python sinh 17,500,000 dòng dữ liệu ecommerce
  (giữ đúng phân phối category/region/payment_method như dataset gốc Week 1-2), ghi theo
  chunk để tránh tràn RAM.
- **Đọc + transform bằng Spark**: đọc CSV bằng `spark.read.csv()`, so sánh thời gian với
  pandas, transform bằng cả DataFrame API và Spark SQL (lặp lại các câu hỏi phân tích đã
  làm ở Week 1 — doanh thu theo category, theo region/năm).
- **Convert sang Parquet/ORC + benchmark**: convert toàn bộ dataset sang 2 định dạng
  columnar, đo dung lượng và tốc độ đọc/query so với CSV.
- **Partitioning theo `order_year`**: ghi lại Parquet có chia partition, benchmark so
  sánh query có filter năm giữa partition và không partition, xác nhận partition pruning
  hoạt động qua execution plan (`explain`).
- **Kết quả chi tiết**: xem đầy đủ số liệu và phân tích tại
  [`docs/spark_architecture.md`](./docs/spark_architecture.md).

### Bảng kết quả chính

| Định dạng             | Dung lượng        | Thời gian đọc | Query aggregation |
| --------------------- | ----------------- | ------------- | ----------------- |
| CSV                   | 1,321.9 MB        | 9.21s         | 3.67s             |
| Parquet               | 390.9 MB (-70.4%) | 0.39s         | 0.64s             |
| ORC                   | 395.8 MB (-70.1%) | 0.51s         | 0.77s             |
| Parquet (partitioned) | 374.6 MB          | —             | —                 |

Partition vs không partition (filter theo 1 năm cụ thể): **0.31s → 0.17s (nhanh hơn 1.8x)**,
xác nhận đúng cơ chế qua `PartitionFilters` trong execution plan.

## Khó khăn gặp phải

- **`createDataFrame()` lỗi `PicklingError: RecursionError`** trên venv Python 3.14 —
  PySpark 3.5.1 dùng cloudpickle chưa hỗ trợ Python 3.14. Xử lý bằng cách tạo venv riêng
  (`venv_spark`, Python 3.12) chỉ dùng cho phần Spark.
- **Mất kết nối Jupyter liên tục khi convert Parquet/ORC** trên 17.5 triệu dòng qua VS
  Code — nghi do WSL2 thiếu RAM khiến kernel bị OOM kill. Xử lý bằng cách convert notebook
  sang script (`jupyter nbconvert --to script`) và chạy trực tiếp qua terminal — ổn định
  hơn hẳn vì không phụ thuộc kết nối websocket của VS Code.

## Việc cần làm tiếp

- Sang Tuần 4: Real-time Streaming với Apache Kafka và Spark Streaming.
- Cân nhắc chuyển dataset sang dạng synthetic ad-request log
  để có tính chất event-theo-thời-gian, phù hợp cho bài tập Kafka producer/consumer và
  nối tiếp được tới dự án Log Processing System.
