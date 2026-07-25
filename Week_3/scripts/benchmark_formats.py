# cd ~/intern_DE/Week_3/scripts
# jupyter nbconvert --to script benchmark_formats.ipynb

# NOTE: File này được sinh tự động từ benchmark_formats.ipynb bằng lệnh:
#   jupyter nbconvert --to script benchmark_formats.ipynb

#!/usr/bin/env python
# coding: utf-8
# # Week 3 - Benchmark định dạng file: CSV vs Parquet vs ORC + Partitioning
# Dùng dữ liệu đã transform từ `spark_transform.ipynb`.

# In[1]:


import os
import time
import shutil
import csv
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = os.path.expanduser("~/intern_DE/Week_3")
CSV_SOURCE = os.path.join(BASE, "data/processed/sales_with_year_csv")
RESULTS_DIR = os.path.join(BASE, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("Week3-BenchmarkFormats") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark


# In[2]:


df = spark.read.csv(CSV_SOURCE, header=True, inferSchema=True)
print(f"Số dòng: {df.count():,}")
df.printSchema()


# ## 1. Convert sang Parquet và ORC (không partition trước)

# In[ ]:


PARQUET_PATH = os.path.join(BASE, "data/processed/sales_parquet")
ORC_PATH = os.path.join(BASE, "data/processed/sales_orc")

start = time.time()
df.write.mode("overwrite").parquet(PARQUET_PATH)
parquet_write_time = time.time() - start
print(f"[parquet] Ghi xong trong {parquet_write_time:.2f} giây")

start = time.time()
df.write.mode("overwrite").orc(ORC_PATH)
orc_write_time = time.time() - start
print(f"[orc] Ghi xong trong {orc_write_time:.2f} giây")


# ## 2. So sánh dung lượng file
# Dùng `du -sh` qua subprocess vì Parquet/ORC lưu thành nhiều file nhỏ trong 1 thư mục (mỗi partition Spark ghi 1 file riêng), không phải 1 file đơn như CSV.

# In[ ]:


def get_dir_size_mb(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            total += os.path.getsize(os.path.join(dirpath, f))
    return total / (1024 ** 2)

csv_size_mb = get_dir_size_mb(CSV_SOURCE)
parquet_size_mb = get_dir_size_mb(PARQUET_PATH)
orc_size_mb = get_dir_size_mb(ORC_PATH)

print(f"CSV:     {csv_size_mb:,.1f} MB")
print(f"Parquet: {parquet_size_mb:,.1f} MB  (giảm {100*(1-parquet_size_mb/csv_size_mb):.1f}% so với CSV)")
print(f"ORC:     {orc_size_mb:,.1f} MB  (giảm {100*(1-orc_size_mb/csv_size_mb):.1f}% so với CSV)")


# ## 3. Benchmark tốc độ đọc + query aggregation trên từng định dạng
# Đo cả thời gian đọc thô lẫn thời gian chạy 1 câu query aggregation thực tế (doanh thu theo category) - đây mới là phép so sánh có ý nghĩa, vì lợi thế thật sự của columnar format (Parquet/ORC) nằm ở việc chỉ đọc đúng cột cần dùng.

# In[ ]:


def benchmark_format(path, fmt):
    start = time.time()
    d = spark.read.format(fmt).load(path) if fmt != "csv" else spark.read.csv(path, header=True, inferSchema=True)
    d.count()
    read_time = time.time() - start

    start = time.time()
    d.groupBy("product_category").agg(F.sum("revenue").alias("total_revenue")).collect()
    query_time = time.time() - start

    return read_time, query_time

results = []
for fmt, path in [("csv", CSV_SOURCE), ("parquet", PARQUET_PATH), ("orc", ORC_PATH)]:
    read_time, query_time = benchmark_format(path, fmt)
    print(f"[{fmt}] đọc: {read_time:.2f}s | query aggregation: {query_time:.2f}s")
    results.append({"format": fmt, "read_seconds": round(read_time, 2), "query_seconds": round(query_time, 2)})


# In[ ]:


with open(os.path.join(RESULTS_DIR, "benchmark_results.csv"), "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["format", "read_seconds", "query_seconds"])
    writer.writeheader()
    writer.writerows(results)
print("Đã ghi vào results/benchmark_results.csv")


# ## 4. Partitioning theo `order_year`
# Ghi lại Parquet nhưng chia thư mục con theo năm - Spark sẽ tự bỏ qua (partition pruning) các thư mục không liên quan khi query có điều kiện lọc trên cột partition, thay vì phải quét toàn bộ dữ liệu.

# In[ ]:


PARTITIONED_PATH = os.path.join(BASE, "data/processed/sales_parquet_partitioned")

start = time.time()
df.write.mode("overwrite").partitionBy("order_year").parquet(PARTITIONED_PATH)
partition_write_time = time.time() - start
print(f"Ghi partitioned parquet trong {partition_write_time:.2f} giây")

# Xem cấu trúc thư mục sinh ra
for entry in sorted(os.listdir(PARTITIONED_PATH))[:5]:
    print(entry)


# ## 5. Benchmark: query có filter năm - partition vs không partition

# In[ ]:


TARGET_YEAR = 2030

# Không partition: Spark phải quét toàn bộ file rồi mới lọc
df_no_partition = spark.read.parquet(PARQUET_PATH)
start = time.time()
count_no_partition = df_no_partition.filter(F.col("order_year") == TARGET_YEAR).count()
time_no_partition = time.time() - start

# Có partition: Spark chỉ đọc đúng thư mục order_year=2030/, bỏ qua các năm khác
df_partitioned = spark.read.parquet(PARTITIONED_PATH)
start = time.time()
count_partitioned = df_partitioned.filter(F.col("order_year") == TARGET_YEAR).count()
time_partitioned = time.time() - start

print(f"Không partition: {count_no_partition:,} dòng, {time_no_partition:.2f}s")
print(f"Có partition:    {count_partitioned:,} dòng, {time_partitioned:.2f}s")
print(f"Nhanh hơn {time_no_partition/time_partitioned:.1f}x")

assert count_no_partition == count_partitioned, "Số dòng phải khớp nhau giữa 2 cách!" 


# ## 6. Xem execution plan để xác nhận partition pruning có xảy ra thật không
# Tìm dòng `PartitionFilters` trong output - nếu có filter push xuống đúng partition, nghĩa là Spark chỉ đọc đúng thư mục cần, không quét toàn bộ.

# In[ ]:


df_partitioned.filter(F.col("order_year") == TARGET_YEAR).explain(mode="formatted")


# In[ ]:


with open(os.path.join(RESULTS_DIR, "partition_benchmark.csv"), "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["scenario", "seconds", "row_count"])
    writer.writerow(["no_partition", round(time_no_partition, 2), count_no_partition])
    writer.writerow(["partitioned_by_year", round(time_partitioned, 2), count_partitioned])
print("Đã ghi vào results/partition_benchmark.csv")


# ## 7. Tổng kết dung lượng cuối cùng

# In[ ]:


partitioned_size_mb = get_dir_size_mb(PARTITIONED_PATH)
print(f"CSV:                  {csv_size_mb:,.1f} MB")
print(f"Parquet:              {parquet_size_mb:,.1f} MB")
print(f"ORC:                  {orc_size_mb:,.1f} MB")
print(f"Parquet (partitioned):{partitioned_size_mb:,.1f} MB")


# In[ ]:


spark.stop()

