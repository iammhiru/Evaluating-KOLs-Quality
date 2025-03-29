# Evaluate-KOL-quality

Dự án này được thiết kế để đánh giá chất lượng của các Key Opinion Leaders (KOLs) sử dụng kiến trúc Lambda. Hệ thống tích hợp với Hadoop, Spark, Kafka và Airflow để xử lý dữ liệu KOL và các bài đăng theo dạng batch và stream.

## Yêu cầu

- Docker: Đảm bảo Docker đã được cài đặt và chạy trên máy của bạn.
- Docker Compose: Đảm bảo Docker Compose cũng đã được cài đặt.

## Cài đặt

Làm theo các bước dưới đây để thiết lập và chạy dự án:

### Bước 1: Cài đặt Docker và Docker Compose

1. **Cài đặt Docker**  
   Bạn có thể làm theo hướng dẫn từ trang chính thức của Docker:  
   https://docs.docker.com/get-docker/

2. **Cài đặt Docker Compose**  
   Để cài đặt Docker Compose, làm theo hướng dẫn chính thức tại đây:  
   https://docs.docker.com/compose/install/

### Bước 2: Xây dựng Image Airflow

Trước khi chạy Docker Compose, bạn cần xây dựng image Airflow.

Chạy lệnh sau trong thư mục dự án:

```bash
docker build ./airflow -t airflow
