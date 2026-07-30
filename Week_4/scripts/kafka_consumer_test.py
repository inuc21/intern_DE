"""
Week 4 - Kafka Consumer test: đọc thử message từ topic ad_requests bằng consumer
Python thuần, để xác nhận producer hoạt động đúng trước khi dùng Spark Streaming.

"""

import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ad_requests"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",  # đọc từ đầu topic nếu chưa có offset lưu trước đó
        group_id="test_consumer_group",
    )

    print(f"[CONSUMER] Đang lắng nghe topic '{TOPIC}'... (Ctrl+C để dừng)")

    received_count = 0
    try:
        for message in consumer:
            received_count += 1
            print(f"[{received_count}] key={message.key} | "
                  f"partition={message.partition} | value={message.value}")

    except KeyboardInterrupt:
        print(f"\n[CONSUMER] Dừng lại. Tổng cộng đã nhận {received_count:,} message")


if __name__ == "__main__":
    main()