# Evaluate-KOL-quality

Xây dựng luồng dữ liệu dựa trên kiến trúc Lambda để thu thập, phân tích và đánh giá KOLs trên Facebook.

## System Architecture

![Design](picture/LambdaArchitecture.png)

## Cài đặt

### Bước 1: Cài đặt Docker và Docker Compose

1. **Cài đặt Docker**  
   https://docs.docker.com/get-docker/

2. **Cài đặt Docker Compose**  
   https://docs.docker.com/compose/install/

### Bước 2: Xây dựng Image Airflow

```bash
docker build ./airflow -t airflow
