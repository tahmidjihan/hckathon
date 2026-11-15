FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY api.py ./
COPY frontend ./frontend

# Optional: include slim data in the image by copying it here (large image).
# Prefer mounting as a volume: -v ./data/unified_slim:/app/data/unified_slim
# If not present, the app falls back to Cloudflare R2 (requires env vars).
# COPY data/unified_slim ./data/unified_slim

EXPOSE 8000

# R2 credentials can be provided via env vars or a mounted .env file:
#   s3_endpoint, access_key, secret_key
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]