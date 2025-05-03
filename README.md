# IT4995 - Bachelor Thesis

## Introduction

<ul>
   <li>Title: Build a system base on Lambda Architecture to ingest, processing and store KOL data</li>
   <li>Project Objective
      <ul>
         <li>Crawling data about KOLs from social flatform (Facebook, X,...)</li>
         <li>Process and store on ingested data</li>
         <li>Evaluting KOLs metric on storing data</li>
      </ul>
   </li>
</ul>

## System pipeline
   <img src="https://github.com/iammhiru/Evaluating-KOLs-Quality/blob/master/picture/LambdaArchitecture.drawio.png">


## Deploy
### 1. Install tools
#### 1.1  Docker  
```
https://docs.docker.com/get-docker/
```

#### 1.2 Kubernetes
```
https://kubernetes.io/releases/download/
```

#### 1.3 Helm 
```
https://helm.sh/docs/intro/install/
```

### 2. Init Kubernetes cluster using minikube
#### 2.1 Set context docker
```sh
docker context set default
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