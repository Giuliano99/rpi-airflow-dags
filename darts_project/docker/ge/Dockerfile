# docker/ge/Dockerfile

# Use ARM-compatible base image for Raspberry Pi
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Install Python packages
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set workdir
WORKDIR /app

# Copy scripts to container
COPY ../../scripts/validation /app/validation

# Default command (override in docker-compose if needed)
CMD ["python", "validation/validate_results_staging.py"]