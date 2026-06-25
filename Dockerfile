# Stage 1: build the React/Vite frontend
FROM node:20-alpine AS frontend

WORKDIR /app
COPY frontend/package.json ./
RUN npm install --no-audit --no-fund

COPY frontend/ ./
RUN npm run build

# Stage 2: FastAPI backend serving /api/* and the static bundle at /
FROM python:3.12-slim

WORKDIR /app

# System deps for aiokafka (librdkafka not required for pure-Python aiokafka,
# but a build base is handy for any future native deps).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./
COPY scripts/ ./scripts/
COPY samples/ ./samples/
COPY --from=frontend /app/dist ./static

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
