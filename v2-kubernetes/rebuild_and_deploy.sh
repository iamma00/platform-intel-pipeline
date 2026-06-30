#!/bin/bash
set -e

echo "Building image..."
docker build -t iamma00/address-classifier:v1 -f docker/Dockerfile .

echo "Pushing image..."
docker push iamma00/address-classifier:v1

echo "Creating PV/PVC..."
kubectl apply -f k8s-manifests/pv-pvc.yaml

echo "Redeploying SparkApplication..."
kubectl delete sparkapplication address-classifier --ignore-not-found

# Wait a few seconds for cleanup
sleep 5

kubectl apply -f k8s-manifests/spark-application.yaml

echo
echo "Current PV/PVC:"
kubectl get pv
kubectl get pvc

echo
echo "Watching pods (Ctrl+C to stop)..."
kubectl get pods -w
