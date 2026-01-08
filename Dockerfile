# EduThreat-CTI API Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/

# Create directories for logs (data dir will be mounted as Railway volume)
RUN mkdir -p logs

# Set environment variables
# Railway persistent storage is mounted at /app/data
# DB will be created/used from persistent volume
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV EDU_CTI_DATA_DIR=/app/data
ENV EDU_CTI_DB_PATH=eduthreat.db

# Expose port
EXPOSE 8000

# Use shell form so PORT env gets read by Python
CMD python -m src.edu_cti.api --host 0.0.0.0
