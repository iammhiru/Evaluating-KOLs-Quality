# IT4995 - Bachelor Thesis

## Introduction

- Title: Build a system base on Lambda Architecture to ingest, processing and store KOL data
- Project Objective
  - Crawling data about KOLs from social flatform (Facebook, X,...)
  - Process and store on ingested data
  - Evaluting KOLs metric on storing data

## System pipeline

![Luồng xử lí dữ liệu](https://github.com/iammhiru/Evaluating-KOLs-Quality/blob/master/picture/LambdaArchitecture.drawio.pdf)

## Deploy

### 1. Install tools

#### 1.1  Docker  

<https://docs.docker.com/get-docker/>

### 2. Prepare to deploy

#### 2.1. Install iceberg jar package

```sh
curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.7.2/iceberg-hive-runtime-1.7.2.jar -O
```

```sh
curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.7.2/iceberg-spark-runtime-3.3_2.12-1.7.2.jar -O
```

```sh
curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar -O
```

#### 2.2. Create Profile Dir (Auto-login Facebook) anđ put to crawler folder

ex: crawler/my_profile

#### 2.3. Update .env file with your profile dir

ex: PROFILE_DIR=my_profile

### 3. Deploy

```sh
docker-compose up -d
```

#### 3.1. Create DB

```sh
docker exec -it hive-metastore bash -c "hive -e \"CREATE DATABASE IF NOT EXISTS db1 LOCATION 'hdfs://namenode:9000/user/hive/warehouse/db1.db';\""
```

You can view result in localhost:8088