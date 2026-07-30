# Streaming Architecture Notes - Week 4

## 1. Tổng quan pipeline

```
Kafka Producer (Python)
        │  sinh ad request log giả lập, gửi liên tục
        ▼
Kafka Broker (Docker, KRaft mode, topic "ad_requests")
        │
        ▼
Spark Structured Streaming (đọc từ Kafka, micro-batch)
        │  parse JSON, windowed aggregation
        ├──► Console (theo dõi trực tiếp khi demo)
        └──► CSV (results/streaming_metrics_csv/, lưu lại phân tích)
```

## 2. Hạ tầng Kafka: chạy bằng Docker, KRaft mode

Kafka được chạy trong container Docker thay vì cài native trên WSL2 — lý do: quản lý gọn
(bật/tắt bằng `docker compose up/down`), không xung đột với Java/Python đã cài cho Spark.

Dùng chế độ **KRaft** (Kafka Raft) — không cần Zookeeper, broker và controller chạy chung
1 container (`combined mode`), phù hợp cho môi trường học tập/local, đơn giản hơn kiến
trúc Kafka cũ (Zookeeper + broker tách riêng).


## 3. Kafka Producer: mô phỏng ad request log

Producer sinh liên tục các bản ghi JSON dạng:
```json
{
  "request_id": "uuid",
  "timestamp": "ISO 8601",
  "publisher_id": "publisher_X",
  "ad_id": "ad_X",
  "user_id": "user_X",
  "response_code": 200,
  "response_time_ms": 160
}
```

- `response_code` được sinh có trọng số (85% là 200, còn lại rải rác 400/404/500/503) để
  mô phỏng tỷ lệ lỗi thực tế của hệ thống ad server.
- `response_time_ms` cố tình cao hơn hẳn (800-3000ms) khi `response_code` là 500/503, mô
  phỏng đúng hành vi thật: request lỗi/timeout thường có độ trễ cao hơn request thành công.
- Dùng `publisher_id` làm **Kafka message key** — đảm bảo mọi request cùng 1 publisher rơi
  vào cùng 1 partition, giữ đúng thứ tự thời gian trong phạm vi 1 publisher (Kafka chỉ đảm
  bảo thứ tự trong nội bộ 1 partition, không đảm bảo thứ tự toàn topic).

Toàn bộ dữ liệu là **synthetic** (giả lập bằng Python, không phải traffic thật) — mục đích
là mô phỏng đúng tính chất event-liên-tục-theo-thời-gian của hệ thống adtech thật, đồng
thời kiểm soát được phân phối dữ liệu để tự kiểm chứng kết quả tính toán có đúng logic hay
không.

## 4. Spark Structured Streaming: đọc Kafka, windowed aggregation

### Micro-batch, không phải streaming từng record

Spark Structured Streaming không xử lý từng message ngay khi tới (khác với true streaming
kiểu Flink/Storm) — mà gom message trong 1 khoảng thời gian (`trigger`) thành 1 micro-batch
rồi xử lý cả batch cùng lúc. Ở bài tập này, `trigger(processingTime="5 seconds")` nghĩa là
cứ mỗi 5 giây, Spark xử lý 1 lần toàn bộ message mới nhận được kể từ lần trước.

### Windowed aggregation: cửa sổ 10 giây, trượt mỗi 5 giây

```python
.groupBy(
    F.window("timestamp", "10 seconds", "5 seconds"),
    "response_code",
)
```

Đây là **sliding window** (cửa sổ trượt) — cửa sổ dài 10 giây nhưng dịch chuyển mỗi 5 giây,
nên các cửa sổ liền kề chồng lấn nhau 5 giây. Khác với **tumbling window** (cửa sổ đóng,
không chồng lấn, ví dụ đúng mỗi 10 giây tính 1 lần độc lập) — sliding window cho cái nhìn
mượt hơn theo thời gian, tại 1 thời điểm có thể thấy dữ liệu thuộc nhiều cửa sổ đang "mở"
cùng lúc.

### Watermark: xử lý dữ liệu đến trễ

```python
.withWatermark("timestamp", "30 seconds")
```

Watermark cho phép Spark chờ tối đa 30 giây cho message đến trễ (do độ trễ mạng, xử lý...)
trước khi "chốt" kết quả 1 cửa sổ và giải phóng bộ nhớ liên quan đến cửa sổ đó. Nếu không
có watermark, Spark phải giữ trạng thái của mọi cửa sổ mãi mãi (vì không biết khi nào mới
hết dữ liệu đến muộn), dẫn đến tràn bộ nhớ khi chạy streaming dài hạn.

### Kết quả thực tế thu được

Chạy producer + Spark Streaming song song, batch 30-31 (sau ~2.5 phút chạy với tốc độ
20 record/giây) cho kết quả:

| window | response_code | request_count | avg_response_time_ms |
|---|---|---|---|
| 03:40:40-03:40:50 | 200 | 163 | 160.0 |
| 03:40:40-03:40:50 | 500 | 8 | 1895.6 |
| 03:40:40-03:40:50 | 503 | 3 | 2817.0 |
| 03:40:45-03:40:55 | 200 | 78 | 165.4 |
| 03:40:45-03:40:55 | 500 | 6 | 1801.8 |

Đối chiếu với thiết kế producer: tỷ lệ `response_code=200` (~85% tổng request/cửa sổ) và
`avg_response_time_ms` của các mã lỗi 500/503 cao gấp 10+ lần so với mã 200 — khớp chính
xác với logic sinh dữ liệu (`RESPONSE_CODE_WEIGHTS`, response_time cao khi lỗi) — xác nhận
toàn bộ pipeline từ producer đến windowed aggregation hoạt động đúng, không có sai lệch dữ
liệu qua các bước xử lý.

## 5. Vấn đề gặp phải: small file problem

Sau khi chạy `foreachBatch` ghi CSV được vài phút, thư mục `results/streaming_metrics_csv/`
có tới hơn 200 file CSV nhỏ. Nguyên nhân: mỗi lần trigger (5 giây/lần) Spark ghi ra file
mới, và vì chạy `local[*]` với nhiều partition song song, mỗi batch còn tách thành nhiều
file `part-0000X` tương ứng số partition.

Đây là vấn đề thực tế phổ biến của Spark Streaming khi ghi ra file sink (nhiều hệ thống
production cũng gặp) — quá nhiều file nhỏ gây tốn overhead metadata, làm chậm hệ thống
file, và khó quản lý.

Xử lý bằng 2 cách kết hợp:
- Thêm `.coalesce(1)` trước khi ghi mỗi batch — gộp về 1 file duy nhất/batch thay vì nhiều
  file theo partition.
- Tăng khoảng `trigger` từ 5 giây lên 15 giây — giảm tần suất ghi, giảm số file sinh ra.

## 6. Lambda vs Kappa Architecture — liên hệ với pipeline đã xây

- **Lambda Architecture**: kết hợp song song 2 luồng xử lý — batch layer (xử lý dữ liệu
  lịch sử, độ chính xác cao nhưng độ trễ lớn) và speed layer (xử lý real-time, độ trễ thấp
  nhưng có thể chưa hoàn toàn chính xác), rồi hợp nhất kết quả ở serving layer.
- **Kappa Architecture**: chỉ dùng 1 luồng streaming duy nhất cho mọi loại xử lý, kể cả khi
  cần tính toán lại dữ liệu lịch sử (bằng cách replay lại stream từ đầu).

Pipeline hiện tại của dự án (Tuần 3: Spark batch xử lý dataset ecommerce tĩnh; Tuần 4:
Spark Streaming xử lý Kafka real-time) đang có cả 2 thành phần tách biệt — về hình thức
gần giống mô hình **Lambda**, nhưng chưa có serving layer hợp nhất kết quả giữa 2 luồng
(2 pipeline hiện chạy độc lập, xử lý 2 dataset khác nhau, không phải cùng 1 nguồn dữ liệu
xử lý theo 2 cách). Ở dự án Tuần 6 (Log Processing System), nếu dùng chung 1 nguồn dữ liệu
log cho cả xử lý batch (report tổng hợp theo ngày) và streaming (alerting real-time), khi
đó mới thực sự áp dụng đúng mô hình Lambda hoặc Kappa.

## 7. Kết luận

- Kafka + Spark Structured Streaming đã chạy thành công đầu-cuối: producer sinh dữ liệu →
  broker lưu trữ → Spark đọc, xử lý theo micro-batch, windowed aggregation → ghi kết quả
  ra console và CSV.
- Số liệu tổng hợp theo cửa sổ thời gian phản ánh đúng logic đã thiết kế ở producer, xác
  nhận tính đúng đắn của toàn bộ pipeline.