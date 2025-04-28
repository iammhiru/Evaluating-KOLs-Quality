# Cài đặt 

1. **Set context**
    ```bash
    docker context use default

2. **Khởi tạo minikube cluster 3 node với 2 worker**
    ```bash
    minikube start --driver=docker --nodes=3 --cpus=2 --memory=6144 -p kol-system
    kubectl label node kol-system-m02 node-role.kubernetes.io/worker=worker & kubectl label nodes kol-system-m02 role=worker
    kubectl label node kol-system-m03 node-role.kubernetes.io/worker=worker & kubectl label nodes kol-system-m03 role=worker

3. **Deploy**
    ```bash
    kubectl create namespace kol-system & kubectl config set-context --current --namespace=kol-system

