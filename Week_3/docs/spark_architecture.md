# Spark Architecture Notes - Week 3

## 1. Kiến trúc Spark áp dụng trong bài tập

Spark chạy theo mô hình Driver - Executor - Cluster Manager:

- **Driver**: process chạy code Python của mình (`SparkSession`), chịu trách nhiệm phân
  tích câu lệnh (DataFrame API / Spark SQL) thành execution plan, chia thành các task,
  và điều phối việc thực thi.
- **Executor**: process thực sự chạy các task, đọc/ghi dữ liệu, thực hiện tính toán.
- **Cluster Manager**: điều phối tài nguyên giữa các executor. Trong bài tập này dùng
  `master("local[*]")` — nghĩa là Driver và toàn bộ Executor chạy chung trên 1 máy (máy
  cá nhân qua WSL2), `[*]` cho phép Spark dùng toàn bộ CPU core sẵn có để giả lập xử lý
  song song, không cần cluster thật.

Vì chạy local mode, mọi khái niệm phân tán (network partition, executor failure...) chưa
thể quan sát trực tiếp — nhưng cơ chế chia task và xử lý song song trên nhiều core vẫn
thể hiện rõ qua Spark UI (`localhost:4040`) khi job đang chạy, thấy được nhiều Stage/Task
chạy đồng thời dù chỉ trên 1 máy.

## 2. Lazy evaluation — vì sao Spark "chậm" hơn pandas ở phép đọc đơn giản

Khi gọi `spark.read.csv(...)`, Spark **chưa đọc dữ liệu ngay** — chỉ ghi nhận kế hoạch.
Dữ liệu chỉ thực sự được đọc khi gặp một **action** (`.count()`, `.collect()`, `.show()`...).
Đây là lazy evaluation, khác hẳn pandas (đọc và load vào RAM ngay lập tức khi gọi
`pd.read_csv()`).

Hệ quả quan sát được trong benchmark thực tế:

|                         | pandas | Spark  |
| ----------------------- | ------ | ------ |
| Đọc 17,500,000 dòng CSV | 14.72s | 16.81s |

Spark **chậm hơn pandas** ở phép đọc đơn lẻ này. Nguyên nhân: overhead khởi tạo JVM +
lên execution plan chưa được bù lại bởi lợi thế song song hóa, vì đây chỉ là 1 thao tác
đọc đơn giản trên 1 máy local — không phải trường hợp Spark được thiết kế để tối ưu.
Lợi thế thật sự của Spark chỉ rõ ràng khi dataset vượt quá RAM máy (pandas sẽ crash,
Spark vẫn chạy được nhờ xử lý theo partition), hoặc khi có nhiều phép biến đổi phức tạp
nối tiếp nhau mà Spark có thể tối ưu toàn bộ execution plan thay vì chạy tuần tự từng
bước như pandas.

## 3. DataFrame API vs Spark SQL

Cả hai cách viết cho cùng 1 kết quả — Spark biên dịch cả hai về chung 1 execution plan
phía sau. DataFrame API:

```python
df.groupBy("product_category").agg(F.sum("revenue").alias("total_revenue"))
```

Spark SQL (sau khi đăng ký `createOrReplaceTempView`):

```python
spark.sql("SELECT product_category, SUM(revenue) AS total_revenue FROM sales GROUP BY product_category")
```

So với SQL thuần trên MySQL ở Week 1 (`SELECT product_category, SUM(revenue) FROM orders
GROUP BY product_category`), cú pháp gần như giống hệt — khác biệt nằm ở phía sau: MySQL
chạy trên 1 engine đơn, Spark SQL biên dịch câu query thành execution plan chạy phân tán
trên nhiều partition/executor.

## 4. So sánh định dạng file: CSV vs Parquet vs ORC

Dữ liệu benchmark trên 17,500,000 dòng (dataset ecommerce sinh ở Tuần 3):

### Dung lượng

| Định dạng                       | Dung lượng | So với CSV |
| ------------------------------- | ---------- | ---------- |
| CSV                             | 1,321.9 MB | —          |
| Parquet                         | 390.9 MB   | -70.4%     |
| ORC                             | 395.8 MB   | -70.1%     |
| Parquet (partitioned theo year) | 374.6 MB   | -71.7%     |

### Tốc độ đọc + query aggregation

| Định dạng | Thời gian đọc | Thời gian query (SUM theo category) |
| --------- | ------------- | ----------------------------------- |
| CSV       | 9.21s         | 3.67s                               |
| Parquet   | 0.39s         | 0.64s                               |
| ORC       | 0.51s         | 0.77s                               |

Parquet đọc nhanh hơn CSV khoảng **24 lần** (9.21s → 0.39s). Lý do: CSV là định dạng
row-based, muốn đọc bất kỳ cột nào cũng phải đọc qua toàn bộ dòng. Parquet/ORC là
columnar format — dữ liệu lưu theo cột, nên khi query chỉ cần vài cột (ví dụ
`product_category`, `revenue`), Spark chỉ đọc đúng các cột đó, bỏ qua toàn bộ cột còn
lại. CSV còn phải tốn thêm thời gian parse text sang kiểu dữ liệu đúng (int, double...)
mỗi lần đọc, trong khi Parquet/ORC đã lưu sẵn dữ liệu ở dạng binary có schema.

Parquet và ORC cho kết quả gần tương đương nhau (Parquet nhỉnh hơn một chút trong lần đo
này). Vì Parquet là định dạng phổ biến hơn trong ecosystem Spark hiện tại (dùng nhiều ở
các công ty lớn xử lý dữ liệu tương tự), Parquet được chọn làm định dạng chính cho các
tuần tiếp theo; ORC chỉ dùng để đối chiếu benchmark, không đầu tư sâu thêm.

## 5. Partitioning theo `order_year`

Ghi lại Parquet nhưng chia thành các thư mục con theo năm:

```
sales_parquet_partitioned/
├── order_year=2022/
├── order_year=2023/
├── order_year=2024/
...
```

Khi query có điều kiện lọc trên cột partition (`WHERE order_year = 2030`), Spark áp dụng
**partition pruning** — chỉ đọc đúng thư mục `order_year=2030/`, bỏ qua hoàn toàn các
thư mục năm khác thay vì quét toàn bộ 17.5 triệu dòng rồi mới lọc.

Kết quả benchmark:

| Kịch bản                                        | Thời gian | Số dòng   |
| ----------------------------------------------- | --------- | --------- |
| Không partition (đọc toàn bộ rồi filter)        | 0.31s     | 1,249,185 |
| Có partition (đọc đúng thư mục order_year=2030) | 0.17s     | 1,249,185 |

Nhanh hơn **1.8 lần**. Chênh lệch không quá lớn vì dataset ở quy mô bài tập này còn nhỏ
và mỗi partition năm đã tương đối gọn (~1.25 triệu dòng/năm trên tổng 14 năm), nhưng cơ
chế partition pruning đã được xác nhận hoạt động đúng qua execution plan:

```
PartitionFilters: [isnotnull(order_year#414), (order_year#414 = 2030)]
```

Dòng `PartitionFilters` này xác nhận Spark thực sự áp dụng filter ngay ở bước chọn file
cần đọc, không phải đọc hết rồi lọc sau — đây là điểm quan trọng hơn con số tuyệt đối,
vì ở dataset thật lớn hơn (hàng chục/hàng trăm GB, nhiều năm dữ liệu), chênh lệch sẽ rõ
rệt hơn nhiều.

## 6. Vấn đề gặp phải trong quá trình thực hành

### Python 3.14 không tương thích với PySpark

`SparkSession.createDataFrame()` báo lỗi `PicklingError: RecursionError: Stack overflow`
khi chạy trên venv Python 3.14 (venv chính dùng cho Week 1-2). Nguyên nhân: PySpark 3.5.1
dùng cloudpickle để serialize dữ liệu gửi sang JVM, chưa hỗ trợ Python 3.14 (bản rất mới).

Xử lý: tạo venv riêng (`venv_spark`) dùng Python 3.12 cho toàn bộ phần Spark, không ảnh
hưởng đến venv chính đang chạy ổn định cho Week 1-2.

### Mất kết nối Jupyter khi chạy job nặng trong VS Code

Khi convert CSV sang Parquet/ORC trên 17.5 triệu dòng qua Jupyter notebook trong VS Code,
kết nối giữa VS Code và kernel bị rớt liên tục ("Attempting to reconnect..."), không thể
reconnect lại được — nghi ngờ do WSL2 thiếu RAM khiến kernel bị OOM kill giữa chừng.

Xử lý: convert notebook sang script và chạy trực tiếp qua terminal thay vì qua Jupyter UI:

```bash
jupyter nbconvert --to script benchmark_formats.ipynb
python benchmark_formats.py
```

Chạy qua terminal ổn định hơn hẳn vì không phụ thuộc kết nối websocket của VS Code, và
job chạy trọn vẹn không bị gián đoạn. Bài học: với các job Spark nặng (xử lý dữ liệu lớn,
thời gian chạy dài), nên ưu tiên chạy qua script/terminal; dùng notebook chủ yếu cho các
bước explore, xem kết quả, hoặc job nhẹ.

## 7. Kết luận

- Spark không phải lúc nào cũng nhanh hơn công cụ đơn giản như pandas — lợi thế chỉ rõ
  ràng khi dữ liệu vượt khả năng xử lý của 1 máy, hoặc khi tận dụng được columnar storage
  và partition pruning.
- Parquet là lựa chọn phù hợp nhất cho pipeline tiếp theo: giảm ~70% dung lượng, đọc
  nhanh hơn CSV tới ~24 lần trong benchmark thực tế.
- Partitioning hợp lý (theo cột hay dùng để filter, ví dụ theo năm hoặc theo ngày) giúp
  Spark bỏ qua phần dữ liệu không liên quan ngay từ bước đọc file, hiệu quả sẽ càng rõ
  rệt khi làm việc với dataset lớn hơn ở các tuần tiếp theo.
