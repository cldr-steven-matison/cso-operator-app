.PHONY: help bootstrap dev backend frontend build deploy clean

IMAGE := cso-operator-app:latest

help:
	@echo "Targets:"
	@echo "  bootstrap   apply backing YAMLs, build whisper image, import NiFi flows"
	@echo "  dev         run port-forwards + backend + frontend for local dev"
	@echo "  backend     run FastAPI with reload"
	@echo "  frontend    run Vite dev server"
	@echo "  build       build the app image into the local Minikube docker daemon"
	@echo "  deploy      build + kubectl apply"
	@echo "  clean       kubectl delete the app (leaves backing stack alone)"

bootstrap:
	bash scripts/bootstrap-stack.sh

dev:
	bash scripts/mac-dev.sh

backend:
	cd backend && uvicorn main:app --reload --host 0.0.0.0 --port 8000

frontend:
	cd frontend && npm run dev

build:
	@eval $$(minikube docker-env) && docker build -t $(IMAGE) .

deploy: build
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/deployment.yaml
	kubectl apply -f k8s/service.yaml
	@echo
	@echo "Open: $$(minikube service cso-operator-app --url)"

clean:
	-kubectl delete -f k8s/service.yaml
	-kubectl delete -f k8s/deployment.yaml
	-kubectl delete -f k8s/configmap.yaml
