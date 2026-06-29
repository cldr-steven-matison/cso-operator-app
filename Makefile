.PHONY: help bootstrap dev backend frontend build deploy clean

IMAGE := cso-operator-app:latest

# STACK controls which backing variant `bootstrap` and `dev` use.
#   STACK=gpu (default) — vLLM + Whisper on NVIDIA GPU (Windows + Mac with passthrough)
#   STACK=cpu           — llama.cpp server + faster-whisper, strict CPU
#
# The toggle only affects vLLM and Whisper. Qdrant, embedding-server,
# Kafka, NiFi, and the app image itself are identical on both paths.
STACK ?= gpu

# MODULES controls which optional tabs/routes are enabled.
# Operator tab is always on. Add any combination of: efm, rag, streamers
#   MODULES=efm,rag,streamers   — full install (matches configmap default)
#   MODULES=streamers           — Operator + Streamers only
#   MODULES=                    — Operator only (bare minimum)
MODULES ?=

help:
	@echo "Targets (STACK=$(STACK) MODULES=$(MODULES)):"
	@echo "  bootstrap   apply backing YAMLs, build whisper image, import NiFi flows"
	@echo "  dev         run port-forwards + backend + frontend for local dev"
	@echo "  backend     run FastAPI with reload"
	@echo "  frontend    run Vite dev server"
	@echo "  build       build the app image into the local Minikube docker daemon"
	@echo "  deploy      build + kubectl apply"
	@echo "  clean       kubectl delete the app (leaves backing stack alone)"
	@echo ""
	@echo "Override the stack:         make bootstrap STACK=cpu"
	@echo "Operator only (default):    make deploy MODULES="
	@echo "Full install (all modules): make deploy MODULES=efm,rag,streamers"
	@echo "Streamers only:             make deploy MODULES=streamers"

bootstrap:
	STACK=$(STACK) bash scripts/bootstrap-stack.sh

dev:
	STACK=$(STACK) bash scripts/mac-dev.sh

backend:
	@cd backend && \
	  if [ ! -d .venv ]; then python3 -m venv .venv; fi && \
	  . .venv/bin/activate && \
	  pip install -q -r requirements.txt && \
	  uvicorn main:app --reload --host 0.0.0.0 --port 8000

frontend:
	cd frontend && npm run dev

build:
	@eval $$(minikube docker-env) && docker build -t $(IMAGE) --build-arg MODULES=$(MODULES) .

deploy:
	MODULES=$(MODULES) bash scripts/deploy.sh

clean:
	-kubectl delete -f k8s/service.yaml
	-kubectl delete -f k8s/deployment.yaml
	-kubectl delete -f k8s/configmap.yaml
