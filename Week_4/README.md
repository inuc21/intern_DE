# Week 4 - Real-time Streaming (Apache Kafka + Spark Streaming)

## Mục tiêu

Xây dựng pipeline xử lý dữ liệu real-time: Kafka producer gửi dữ liệu liên tục, Spark
Structured Streaming đọc và xử lý theo cửa sổ thời gian, hiểu khái niệm Lambda vs Kappa
architecture.

## Đã học

- **Kafka core concepts**: producer, consumer, topic, partition, broker — và chế độ KRaft
  (không cần Zookeeper, broker kiêm luôn vai trò controller).
- **Spark Structured Streaming**: xử lý theo micro-batch (không phải xử lý từng record
  ngay lập tức), điều khiển bằng `trigger(processingTime=...)`.
- **Windowed aggregation**: sliding window (cửa sổ trượt, có chồng lấn) khác với tumbling
  window (cửa sổ đóng, không chồng lấn).
- **Watermark**: cơ chế cho phép chờ dữ liệu đến trễ trong 1 khoảng thời gian trước khi
  chốt kết quả cửa sổ, tránh giữ trạng thái vô hạn gây tràn bộ nhớ.
- **Lambda vs Kappa Architecture**: Lambda kết hợp song song batch + speed layer; Kappa chỉ
  dùng 1 luồng streaming duy nhất kể cả khi cần xử lý lại dữ liệu lịch sử.

## Đã thực hành

- **Hạ tầng Kafka**: chạy bằng Docker (KRaft mode, image `apache/kafka`), tạo topic
  `ad_requests` với 3 partition.
- **Kafka Producer**: script Python sinh liên tục ad request log giả lập (20 record/giây),
  có phân phối `response_code` và `response_time_ms` mô phỏng traffic thực tế (đa số
  thành công, số ít lỗi với độ trễ cao hơn).
- **Kafka Consumer test**: xác nhận producer hoạt động đúng bằng consumer Python thuần
  trước khi đưa Spark vào.
- **Spark Structured Streaming**: đọc trực tiếp từ Kafka, parse JSON, tính windowed
  aggregation (số request/response_code/response_time trung bình theo cửa sổ 10 giây,
  trượt mỗi 5 giây), ghi kết quả ra console và CSV.
- **Kết quả chi tiết và giải thích kỹ thuật**: xem tại
  [`docs/streaming_architecture.md`](./docs/streaming_architecture.md).

### Kết quả mẫu (batch thực tế đã chạy)

| response_code | request_count | avg_response_time_ms |
|---|---|---|
| 200 | 163 | 160.0 |
| 500 | 8 | 1895.6 |
| 503 | 3 | 2817.0 |

Số liệu khớp đúng với thiết kế producer (85% request thành công, request lỗi có độ trễ
cao gấp 10+ lần) — xác nhận pipeline xử lý đúng logic từ đầu đến cuối.

## Khó khăn gặp phải

- **Small file problem**: ghi CSV theo `foreachBatch` mỗi 5 giây sinh ra hơn 200 file nhỏ
  chỉ sau vài phút chạy. Xử lý bằng `.coalesce(1)` trước khi ghi mỗi batch và tăng khoảng
  trigger lên 15 giây để giảm tần suất ghi.

## Việc cần làm tiếp

- Sang Tuần 5: Workflow & Integration với Apache Airflow, NiFi, tích hợp API.