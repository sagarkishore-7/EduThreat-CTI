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

# Create directories and copy database
RUN mkdir -p data logs
COPY data/eduthreat.db ./data/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV EDU_CTI_DB_PATH=/app/data/eduthreat.db

# Expose port
EXPOSE 8000

# Use shell form so $PORT gets expanded by shell
CMD python -m src.edu_cti.api --host 0.0.0.0
