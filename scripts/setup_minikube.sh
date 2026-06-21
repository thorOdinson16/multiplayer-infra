#!/bin/bash
echo "Setting up minikube..."
minikube start --cpus=8 --memory=16g --disk-size=40g
kubectl create namespace game-platform
kubectl create namespace monitoring
kubectl create namespace infra
