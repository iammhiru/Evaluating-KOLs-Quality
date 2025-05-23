# IT4995 - Bachelor Thesis

## Introduction

- Title: Build a system base on Lambda Architecture to ingest, processing and store KOL data
- Project Objective
  - Crawling data about KOLs from social flatform (Facebook, X,...)
  - Process and store on ingested data
  - Evaluting KOLs metric on storing data

## System pipeline

![Luồng xử lí dữ liệu](https://github.com/iammhiru/Evaluating-KOLs-Quality/blob/master/picture/LambdaArchitecture.drawio.png)

## Deploy

### 1. Install tools

#### 1.1  Docker  

<https://docs.docker.com/get-docker/>

#### 1.2 Kubernetes

<https://kubernetes.io/releases/download/>

#### 1.3 Helm

<https://helm.sh/docs/intro/install/>

### 2. Init Kubernetes cluster using minikube

#### 2.1 Set context docker

```sh
docker context use default
```

#### 2.2 Create cluster

```sh
minikube start --driver=docker --nodes=3 --cpus=2 --memory=6144 -p kol-system
```

#### 2.3 Label node

```sh
kubectl label node kol-system-m02 node-role.kubernetes.io/worker=worker & kubectl label nodes kol-system-m02 role=worker
```

```sh
kubectl label node kol-system-m03 node-role.kubernetes.io/worker=worker & kubectl label nodes kol-system-m03 role=worker
```

### 3. Deploy

#### 3.1 Create namespace

```sh
kubectl create namespace kol-system & kubectl config set-context --current --namespace=kol-system
```

#### 3.2 Deploy Hadoop

```sh
helm install hadoop ./kubernetes/hadoop
```

#### 3.3 Deploy Kafka

```sh
kubectl create -f ./kubernetes/kafka-sm
```

#### 3.4 Deploy Spark

```sh
helm install spark ./kubernetes/spark
```

#### 3.4 Setup Hive-metastore

```sh
helm dependency build ./kubernetes/hive-metastore
helm install hive-metastore ./kubernetes/hive-metastore
```

#### 3.5 Deploy Trino

```sh
helm install trino ./kubernetes/trino
```

#### 3.6 Deploy Superset

```sh
helm install superset ./kubernetes/superset
```

#### 3.7 Deploy Airflow

```sh
helm install airflow ./kubernetes/airflow
```

### 4. Init system

```sh
docker exec -it hive-metastore bash
hive -e "CREATE DATABASE IF NOT EXISTS db1 LOCATION 'hdfs://namenode:9000/user/hive/warehouse/db1.db';"
```

```sh
docker exec -it trino bash
trino

```