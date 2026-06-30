#!/bin/bash
set -e

echo "Deleting SparkApplication..."
kubectl delete sparkapplication address-classifier --ignore-not-found

echo "Deleting driver/executor pods (if any)..."
kubectl delete pod -l spark-app-name=address-classifier --ignore-not-found

echo "Deleting PV/PVC..."
kubectl delete -f k8s-manifests/pv-pvc.yaml --ignore-not-found

echo
echo "Remaining resources:"
kubectl get sparkapplication || true
kubectl get pods
kubectl get pvc
kubectl get pv
