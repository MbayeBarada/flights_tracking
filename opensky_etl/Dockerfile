# Use Python 3.9 as the base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables for Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy everything to the working directory
COPY . .

# Install specific versions of numpy and pandas first to avoid compatibility issues
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir wheel && \
    pip install --no-cache-dir numpy==1.23.5 && \
    pip install --no-cache-dir pandas==1.5.3 && \
    pip install --no-cache-dir -r requirements.txt

# Create logs directory
RUN mkdir -p logs

# Make the wrapper executable
RUN chmod +x docker_wrapper.py


CMD ["python", "docker_wrapper.py"]