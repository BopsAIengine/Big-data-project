from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import datetime
import sys
import os  # <-- 1. THÊM DÒNG NÀY

# 2. THÊM DÒNG NÀY (Trỏ đến thư mục cha của 'bin')
os.environ['HADOOP_HOME'] = 'C:\\Users\\VinhLaptop\\Hadoop'

try:
    # Cho phép bạn truyền ngày vào, ví dụ: spark-submit ... ingest_batch.py 2025-10-25
    yesterday_str = sys.argv[1]
    print(f"Đang chạy backfill cho ngày: {yesterday_str}")
except IndexError:
    # Nếu không truyền, tự lấy ngày hôm qua
    yesterday_str = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    print(f"Bắt đầu xử lý dữ liệu cho ngày: {yesterday_str}")

# Tên file JAR bạn đã tải
MYSQL_DRIVER_JAR = "mysql-connector-j-9.5.0.jar"

# --- CẤU HÌNH KẾT NỐI (CHỌN 1 TRONG 2) ---

# --- A. Cấu hình DEBUG (Chạy từ PyCharm) ---
RUN_MODE = "DEBUG"
MYSQL_HOST = "127.0.0.1"  # Localhost
MYSQL_PORT = "3307"        # Cổng bạn forward (hoặc 3306)
MYSQL_DB = "ecommerce_db"
MYSQL_USER = "root"
MYSQL_PASS = "123456789"
HDFS_URI = "hdfs://127.0.0.1:8020"

# --- B. Cấu hình PRODUCTION (Chạy trong K8s) ---
# RUN_MODE = "PROD"
# MYSQL_HOST = "mysql-service"
# MYSQL_PORT = "3306"
# MYSQL_DB = "ecommerce_db"
# MYSQL_USER = "root"
# MYSQL_PASS = "123456789"
# HDFS_URI = "hdfs://hdfs-namenode-service:8020"

MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
HDFS_PATH = f"{HDFS_URI}/data/sales/"


def main():
    print(f"--- Chạy ở chế độ: {RUN_MODE} ---")
    print(f"Kết nối MySQL tại: {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"Ghi HDFS tại: {HDFS_URI}")

    spark = SparkSession.builder \
        .appName(f"Batch_Ingest_{yesterday_str}") \
        .config("spark.jars", MYSQL_DRIVER_JAR) \
        .getOrCreate()

    try:
        query = f"""
        (SELECT
            od.order_detail_id,
            od.order_id,
            od.product_id,
            od.quantity,
            od.price_at_purchase,
            o.customer_id,
            DATE(o.order_date) as order_date_col
        FROM Order_Details od
        JOIN Orders o ON od.order_id = o.order_id
        WHERE DATE(o.order_date) = '{yesterday_str}'
        ) AS sales_data_yesterday
        """

        print("Đang đọc dữ liệu từ MySQL...")

        mysql_df = spark.read \
            .format("jdbc") \
            .option("url", MYSQL_URL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", query) \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASS) \
            .load()

        record_count = mysql_df.count()
        print(f"Đã đọc thành công {record_count} dòng dữ liệu.")

        if record_count == 0:
            print(f"Không có dữ liệu cho ngày {yesterday_str}. Dừng script.")
            return

        # Thêm cột date để partition
        final_df = mysql_df.withColumn("date", lit(yesterday_str))

        print(f"Đang ghi {record_count} dòng vào HDFS tại: {HDFS_PATH}")

        final_df.write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("date") \
            .save(HDFS_PATH)

        print("--- GHI DỮ LIỆU HOÀN TẤT! ---")

    except Exception as e:
        print("\n--- !!! ĐÃ XẢY RA LỖI !!! ---")
        print(e)

    finally:
        spark.stop()
        print("Spark Session đã đóng.")


if __name__ == "__main__":
    main()
