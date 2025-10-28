import mysql.connector
from mysql.connector import Error
from faker import Faker
import random
from datetime import datetime
import time  # Thêm thư viện time

# --- THAY ĐỔI 1: CẬP NHẬT DB_CONFIG ---
# (Giữ nguyên)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'port': 3307,
    'password': '123456789',  # <--- Mật khẩu từ file YAML
    'database': 'ecommerce_db'  # <--- Database từ file YAML
}

# --- CẤU HÌNH SỐ LƯỢNG DỮ LIỆU ---
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 1000  # Sẽ tạo ra ~2500 bản ghi trong Order_Details

# Khởi tạo Faker
fake = Faker('vi_VN')  # Dùng dữ liệu giả Tiếng Việt

# Định nghĩa các bảng (Không thay đổi)
TABLES = {}
TABLES['Customers'] = (
    "CREATE TABLE IF NOT EXISTS Customers ("
    "   customer_id INT AUTO_INCREMENT PRIMARY KEY,"
    "   name VARCHAR(100),"
    "   email VARCHAR(100) UNIQUE,"
    "   address TEXT"
    ") ENGINE=InnoDB")

TABLES['Products'] = (
    "CREATE TABLE IF NOT EXISTS Products ("
    "   product_id INT AUTO_INCREMENT PRIMARY KEY,"
    "   name VARCHAR(255),"
    "   category VARCHAR(50),"
    "   price DECIMAL(10, 2)"
    ") ENGINE=InnoDB")

TABLES['Orders'] = (
    "CREATE TABLE IF NOT EXISTS Orders ("
    "   order_id INT AUTO_INCREMENT PRIMARY KEY,"
    "   customer_id INT,"
    "   order_date DATETIME,"
    "   status VARCHAR(20),"
    "   FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)"
    ") ENGINE=InnoDB")

TABLES['Order_Details'] = (
    "CREATE TABLE IF NOT EXISTS Order_Details ("
    "   order_detail_id INT AUTO_INCREMENT PRIMARY KEY,"
    "   order_id INT,"
    "   product_id INT,"
    "   quantity INT,"
    "   price_at_purchase DECIMAL(10, 2),"
    "   FOREIGN KEY (order_id) REFERENCES Orders(order_id),"
    "   FOREIGN KEY (product_id) REFERENCES Products(product_id)"
    ") ENGINE=InnoDB")


def create_tables(cursor):
    for table_name, table_sql in TABLES.items():
        try:
            print(f"Đang tạo bảng {table_name}... ", end='')
            cursor.execute(table_sql)
            print("OK")
        except Error as err:
            print(err.msg)


# --- (MỚI) HÀM DỌN DẸP BẢNG ---
def truncate_tables(cursor):
    """Xóa sạch dữ liệu và reset AUTO_INCREMENT của các bảng."""
    print("Đang dọn dẹp (TRUNCATE) các bảng cũ...")
    TABLE_NAMES_TO_TRUNCATE = ['Order_Details', 'Orders', 'Customers', 'Products']
    try:
        # Tắt kiểm tra khóa ngoại
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        for table_name in TABLE_NAMES_TO_TRUNCATE:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            print(f"Đã TRUNCATE bảng {table_name}.")

        # Bật lại kiểm tra khóa ngoại
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        print("OK. Đã dọn dẹp xong.")
    except Error as err:
        print(f"Lỗi khi TRUNCATE bảng: {err}")
        # Luôn bật lại khóa ngoại dù có lỗi
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        raise err  # Ném lỗi ra ngoài để dừng script


# Hàm chèn Customers (Không thay đổi)
def insert_customers(cursor):
    print(f"Đang chèn {NUM_CUSTOMERS} khách hàng...")
    customers = []
    for _ in range(NUM_CUSTOMERS):
        customers.append((fake.name(), fake.unique.email(), fake.address()))

    sql = "INSERT INTO Customers (name, email, address) VALUES (%s, %s, %s)"
    cursor.executemany(sql, customers)
    print("OK")


# --- (SỬA) HÀM CHÈN PRODUCTS ---
def insert_products(cursor):
    """Chèn sản phẩm, không trả về gì cả."""
    print(f"Đang chèn {NUM_PRODUCTS} sản phẩm...")
    categories = ['Điện tử', 'Thời trang', 'Gia dụng', 'Sách', 'Thể thao']
    products = []
    # Không cần cache ở đây nữa

    for i in range(1, NUM_PRODUCTS + 1):
        name = fake.bs().title()
        category = random.choice(categories)
        price = round(random.uniform(50000, 5000000), -3)
        products.append((name, category, price))

    sql = "INSERT INTO Products (name, category, price) VALUES (%s, %s, %s)"
    cursor.executemany(sql, products)
    print("OK")
    # Không return cache nữa


# --- (SỬA LỚN) HÀM CHÈN ORDERS VÀ DETAILS ---
def insert_orders_and_details(cursor):
    """
    Chèn Orders và Details.
    Hàm này tự lấy ID và giá từ DB thay vì dựa vào tham số.
    """
    print(f"Đang chèn {NUM_ORDERS} đơn hàng và chi tiết đơn hàng...")

    # --- SỬA 1: Lấy ID và giá sản phẩm THỰC TẾ từ DB ---
    print("Đang lấy thông tin sản phẩm và khách hàng từ DB...")
    product_price_cache = {}
    cursor.execute("SELECT product_id, price FROM Products")
    for (product_id, price) in cursor:
        product_price_cache[product_id] = price
    # Lấy list các ID sản phẩm thực tế
    product_ids = list(product_price_cache.keys())

    # --- SỬA 2: Lấy ID khách hàng THỰC TẾ từ DB ---
    customer_ids = []
    cursor.execute("SELECT customer_id FROM Customers")
    for (customer_id,) in cursor:  # (customer_id,) là một tuple 1 phần tử
        customer_ids.append(customer_id)

    if not product_ids or not customer_ids:
        print("LỖI: Không tìm thấy Customers hoặc Products. Dừng chèn Orders.")
        return

    print("OK. Bắt đầu chèn Orders...")

    sql_order = "INSERT INTO Orders (customer_id, order_date, status) VALUES (%s, %s, %s)"
    sql_detail = "INSERT INTO Order_Details (order_id, product_id, quantity, price_at_purchase) VALUES (%s, %s, %s, %s)"

    order_details_to_insert = []

    for _ in range(NUM_ORDERS):
        # 1. Tạo 1 Order
        # --- SỬA 3: Lấy random ID từ list thực tế ---
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-2y', end_date='now')
        status = random.choice(['Completed', 'Shipped', 'Pending', 'Cancelled'])

        cursor.execute(sql_order, (customer_id, order_date, status))
        order_id = cursor.lastrowid

        # 2. Tạo 1-5 Order_Details cho Order đó
        num_items_in_order = random.randint(1, 5)
        for _ in range(num_items_in_order):
            # --- SỬA 4: Lấy random ID từ list thực tế ---
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 3)
            price_at_purchase = product_price_cache[product_id]  # Lấy giá từ cache

            order_details_to_insert.append((order_id, product_id, quantity, price_at_purchase))

    # Dùng executemany để chèn tất cả details 1 lúc cho nhanh
    cursor.executemany(sql_detail, order_details_to_insert)
    print(f"OK. Đã chèn {len(order_details_to_insert)} chi tiết đơn hàng.")


# --- (SỬA) HÀM MAIN ---
def try_connect():
    """Hàm thử kết nối với cơ sở dữ liệu."""
    db_conn = None
    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        print("Kết nối MySQL thành công!")
        return db_conn
    except Error as err:
        print(f"Chưa thể kết nối tới MySQL: {err}")
        print("Thử lại sau 5 giây...")
        return None


def main():
    db_conn = None
    cursor = None

    print("Đang chờ kết nối tới MySQL (thông qua port-forward)...")

    while db_conn is None:
        db_conn = try_connect()
        if db_conn is None:
            time.sleep(5)

    try:
        cursor = db_conn.cursor()

        # 1. Tạo các bảng (nếu chưa có)
        create_tables(cursor)

        # 2. (MỚI) Luôn dọn dẹp dữ liệu cũ để bắt đầu lại
        truncate_tables(cursor)

        # 3. Chèn dữ liệu mới
        insert_customers(cursor)
        insert_products(cursor)  # Sửa: Không cần biến
        insert_orders_and_details(cursor)  # Sửa: Không cần tham số

        # Commit (lưu) tất cả thay đổi
        db_conn.commit()
        print("\n--- TẠO DỮ LIỆU DEMO THÀNH CÔNG! ---")

    except Error as err:
        print(f"\nĐÃ XẢY RA LỖI: {err}")
        if db_conn:
            db_conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if db_conn:
            db_conn.close()
        print("Đã đóng kết nối MySQL.")


if __name__ == "__main__":
    main()

