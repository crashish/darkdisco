FROM python:3.12-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    unrar-free \
    tesseract-ocr \
    tesseract-ocr-eng \
    # weasyprint / PDF reporting dependencies
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libcairo2 \
    libgdk-pixbuf-2.0-0 \
    libffi-dev \
    fonts-liberation \
    fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application source (overridden by volume mount in dev)
COPY src/ /app/src/

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
