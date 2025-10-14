# Big-data-project
Thu thập dữ liệu  Khám phá dữ liệu (exploratory analysis)  Chuẩn bị và xử lý dữ liệu  Phân tích với các công cụ Big Data  Trình bày và báo cáo kết quả
Tổng quát về dự án này:

Thiết kế nền tảng dữ liệu sử dụng MySQL làm cơ sở dữ liệu OLTP và MongoDB làm cơ sở dữ liệu NoSQL.

Thiết kế và triển khai kho dữ liệu và tạo báo cáo từ dữ liệu.

Thiết kế bảng báo cáo phản ánh các số liệu chính của doanh nghiệp.

Trích xuất dữ liệu từ cơ sở dữ liệu OLTP và NoSQL, chuyển đổi và tải dữ liệu vào kho dữ liệu, sau đó tạo đường dẫn ETL.

Và cuối cùng, tạo kết nối Spark với kho dữ liệu, sau đó triển khai mô hình học máy.

Dự án chia làm 6 Mô-đun để dễ dàng xử lý:

Trong Mô-đun 1, bạn sẽ thiết kế cơ sở dữ liệu OLTP cho một trang web thương mại điện tử, điền dữ liệu được cung cấp vào Cơ sở dữ liệu OLTP và tự động xuất dữ liệu gia tăng hàng ngày vào kho dữ liệu.

Trong Mô-đun 2, bạn sẽ thiết lập cơ sở dữ liệu NoSQL để lưu trữ dữ liệu danh mục cho trang web thương mại điện tử, tải dữ liệu danh mục thương mại điện tử vào cơ sở dữ liệu NoSQL và truy vấn dữ liệu danh mục thương mại điện tử trong cơ sở dữ liệu NoSQL.

Trong Mô-đun 3, bạn sẽ thiết kế lược đồ cho kho dữ liệu dựa trên lược đồ của cơ sở dữ liệu OLTP và NoSQL. Sau đó, bạn sẽ tạo lược đồ và tải dữ liệu vào các bảng dữ liệu thực tế và bảng chiều, tự động hóa việc chèn dữ liệu gia tăng hàng ngày vào kho dữ liệu, đồng thời tạo các khối và bảng cuộn để báo cáo dễ dàng hơn.

Trong Mô-đun 4, bạn sẽ tạo nguồn dữ liệu Cognos trỏ đến bảng kho dữ liệu, tạo biểu đồ thanh về doanh số bán điện thoại di động theo quý, tạo biểu đồ tròn về doanh số bán hàng điện tử theo danh mục và tạo biểu đồ đường về tổng doanh số bán hàng theo tháng trong năm 2020.

Trong Mô-đun 5, bạn sẽ trích xuất dữ liệu từ các cơ sở dữ liệu OLTP, NoSQL và MongoDB sang định dạng CSV. Sau đó, bạn sẽ chuyển đổi dữ liệu OLTP cho phù hợp với lược đồ kho dữ liệu và tải dữ liệu đã chuyển đổi vào kho dữ liệu. Cuối cùng, bạn sẽ kiểm tra xem dữ liệu đã được tải đúng cách chưa.

Trong Mô-đun 6 và cũng là Mô-đun cuối cùng, bạn sẽ sử dụng các kỹ năng về Phân tích dữ liệu lớn để tạo kết nối Spark với kho dữ liệu, sau đó triển khai mô hình học máy trên SparkML để lập dự báo doanh số.
