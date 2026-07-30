"""
Week 4 - Kafka Producer: sinh ad request log giả lập, gửi liên tục vào Kafka.

Mô phỏng traffic ad request thực tế của hệ thống adtech: mỗi request có timestamp,
publisher, ad, user, response_code, response_time_ms. Chạy liên tục cho đến khi
Ctrl+C, tốc độ gửi có thể điều chỉnh qua RECORDS_PER_SECOND.

"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ad_requests"
RECORDS_PER_SECOND = 20  # điều chỉnh tốc độ mô phỏng traffic

PUBLISHERS = [f"publisher_{i}" for i in range(1, 11)]
ADS = [f"ad_{i}" for i in range(1, 51)]
# Trọng số phân phối response_code giống traffic thật: đa số 200, số ít lỗi
RESPONSE_CODES = [200, 204, 400, 404, 500, 503]
RESPONSE_CODE_WEIGHTS = [0.85, 0.05, 0.03, 0.03, 0.03, 0.01]


def generate_event() -> dict:
    now = datetime.now(timezone.utc)
    response_code = random.choices(RESPONSE_CODES, weights=RESPONSE_CODE_WEIGHTS, k=1)[0]

    # response_time cao hơn bất thường khi lỗi 500/503 -- mô phỏng timeout/overload thật
    if response_code in (500, 503):
        response_time_ms = random.randint(800, 3000)
    else:
        response_time_ms = random.randint(20, 300)

    return {
        "request_id": str(uuid.uuid4()),
        "timestamp": now.isoformat(),
        "publisher_id": random.choice(PUBLISHERS),
        "ad_id": random.choice(ADS),
        "user_id": f"user_{random.randint(1, 20000)}",
        "response_code": response_code,
        "response_time_ms": response_time_ms,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"[PRODUCER] Bắt đầu gửi vào topic '{TOPIC}' tại {RECORDS_PER_SECOND} record/giây")
    print("[PRODUCER] Nhấn Ctrl+C để dừng")

    sent_count = 0
    interval = 1.0 / RECORDS_PER_SECOND

    try:
        while True:
            event = generate_event()
            # dùng publisher_id làm key -- đảm bảo message cùng publisher vào cùng partition,
            # giữ đúng thứ tự thời gian trong phạm vi 1 publisher
            producer.send(TOPIC, key=event["publisher_id"], value=event)
            sent_count += 1

            if sent_count % 100 == 0:
                print(f"[PRODUCER] Đã gửi {sent_count:,} record")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n[PRODUCER] Dừng lại. Tổng cộng đã gửi {sent_count:,} record")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()