"""
Week 4 - Spark Structured Streaming: đọc ad request log từ Kafka, tổng hợp
theo cửa sổ thời gian (windowed aggregation) real-time.

Tính toán mỗi cửa sổ 10 giây (trượt mỗi 5 giây):
- Số request/phút quy đổi theo cửa sổ
- Phân phối response_code
- response_time_ms trung bình

"""

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "ad_requests"

BASE = os.path.expanduser("~/intern_DE/Week_4")
CHECKPOINT_DIR = os.path.join(BASE, "data/checkpoints")
RESULTS_PATH = os.path.join(BASE, "results/streaming_metrics_csv")

os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)

# Schema JSON khớp đúng với event sinh ra ở kafka_producer.py
EVENT_SCHEMA = StructType([
    StructField("request_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("publisher_id", StringType()),
    StructField("ad_id", StringType()),
    StructField("user_id", StringType()),
    StructField("response_code", IntegerType()),
    StructField("response_time_ms", IntegerType()),
])


def main():
    spark = SparkSession.builder \
        .appName("Week4-KafkaStreaming") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # 1. Đọc raw stream từ Kafka -- mỗi message có key/value dạng bytes
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")  # chỉ đọc message MỚI kể từ lúc job start
        .load()
    )

    # 2. Parse JSON từ cột value (đang ở dạng binary)
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", EVENT_SCHEMA).alias("data"))
        .select("data.*")
        # dùng thời gian xử lý của Spark làm watermark reference thay vì tin cậy tuyệt đối
        # timestamp trong message (phòng trường hợp producer/consumer lệch giờ)
        .withWatermark("timestamp", "30 seconds")
    )

    # 3. Windowed aggregation: cửa sổ 10 giây, trượt mỗi 5 giây
    windowed_metrics = (
        parsed_stream
        .groupBy(
            F.window("timestamp", "10 seconds", "5 seconds"),
            "response_code",
        )
        .agg(
            F.count("*").alias("request_count"),
            F.round(F.avg("response_time_ms"), 1).alias("avg_response_time_ms"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "response_code",
            "request_count",
            "avg_response_time_ms",
        )
    )

    # 4. Ghi ra console để theo dõi trực tiếp khi demo
    console_query = (
        windowed_metrics.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # 5. Đồng thời ghi ra CSV để lưu lại phân tích sau (dùng foreachBatch vì
    #    CSV sink không hỗ trợ outputMode "update" trực tiếp)
    def write_batch_to_csv(batch_df, batch_id):
        if batch_df.count() > 0:
            (
                batch_df.coalesce(1)
                .withColumn("batch_id", F.lit(batch_id))
                .write.mode("append")
                .option("header", "true")
                .csv(RESULTS_PATH)
            )
    csv_query = (
        windowed_metrics.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_csv)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="15 seconds")
        .start()
    )

    print(f"[STREAMING] Đang lắng nghe topic '{TOPIC}'... (Ctrl+C để dừng)")
    print(f"[STREAMING] Kết quả đang ghi vào {RESULTS_PATH}")
    print("[STREAMING] Xem Spark UI tại http://localhost:4040")

    try:
        console_query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[STREAMING] Đang dừng...")
        console_query.stop()
        csv_query.stop()
        spark.stop()


if __name__ == "__main__":
    main()