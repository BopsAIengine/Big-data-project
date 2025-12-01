Dự án bigdata lấy ý tưởng từ https://youtu.be/obNqcFM866M?si=v4Vkeb08q4v-XPcK


Kiến trúc ETL lấy cảm hứng từ https://www.databricks.com/glossary/medallion-architecture phân tách các quy trình thành nhiều giai đoạn dữ liệu,lưu trữ dữ liệu thô được trích xuất để đối chiếu dữ liệu sau này và cho phép kiến ​​trúc chịu lỗi bằng cách khôi phục dữ liệu một cách thuận tiện

<img width="777" height="244" alt="image" src="https://github.com/user-attachments/assets/fc3aca09-e14f-4b6f-9113-9e746864a5f4" />

Việc nhập dữ liệu từlớp Đồng sang lớp Bạc có thể được thực hiện bằng một khuôn khổ tùy chỉnh. Dùng pyspark

Triển khai Data pipeline:

<img width="855" height="449" alt="image" src="https://github.com/user-attachments/assets/bdbb7934-e7c7-4297-9a72-7c347bff23ca" />

Dự án này sử dụng khung Ingestion Pyspark chịu trách nhiệm về Ingestion (Ingestion là một phần xử lý giữa vùng đồng và vùng bạc , chịu trách nhiệm làm sạch hoặc chuẩn bị dữ liệu thô để sẵn sàng tổng hợp)

Thiết lập mối quan hệ code:
Docker compose sẽ sao chép dữ liệu thô: data/uncleaned_data.csv và docker/setup.sql vào một container cơ sở dữ liệu postgres nguồn để mô phỏng nguồn dữ liệu dưới dạng RDBMS. Với vai trò setup.sqllà điểm vào của container, cơ sở dữ liệu postgres nguồn sẽ tự động khởi tạo bảng và tải dữ liệu vào. Tất cả các bước thực thi được định nghĩa trong docker/postgres.Dockerfile
Docker compose sẽ sao chép docker/target.sql vào một container cơ sở dữ liệu postgres đích để mô phỏng cơ sở dữ liệu đích dưới dạng RDBMS. Với target.sql làm điểm vào của container, cơ sở dữ liệu postgres đích sẽ khởi tạo một bảng trống đang chờ các quy trình ETL. Tất cả các bước thực thi được định nghĩa trong docker/postgres-target.Dockerfile
Docker compose sẽ thực hiện những điều sau:
Gắn khối lượng thông tin xác thực (GCP-ADC, AWS) vào bộ lập lịch luồng không khí và các container máy chủ web. Nó cũng gắn các thành phần cần thiết cho luồng không khí như logsthư mục src/config/variables.json và src/config/connections.json , sẽ được trình bày chi tiết hơn trong Chủ đề 2.1.3 Thiết lập Kết nối và Biến Airflow.
Gắn docker/airflow-entrypoint.sh vào bộ lập lịch Airflow và container máy chủ web, sau đó sử dụng nó làm tập lệnh điểm vào để nhập các tệp variables.json và connections.json đã gắn kết vào thời gian chạy khi khởi động lần đầu. Các bước liên quan đến xác thực cũng sẽ được giải thích trong Chủ đề 2.1.4 Điểm vào Airflow .
Sử dụng airflow.properties để cấu hình các biến môi trường của vùng chứa Airflow (hoạt động như .envtệp trong vùng chứa airflow)
Cài đặt các dependency python của airflow được chỉ định trong pyproject.toml được thực thi bởi airflow.Dockerfile với poetry.lock 

