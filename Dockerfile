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

# Create directories and copy database to /app/db (not /app/data to avoid volume mount)
RUN mkdir -p db logs
COPY data/eduthreat.db ./db/

# Set environment variables
# DB_PATH = DATA_DIR / EDU_CTI_DB_PATH, so we set DATA_DIR to /app/db
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV EDU_CTI_DATA_DIR=/app/db
ENV EDU_CTI_DB_PATH=eduthreat.db

# Expose port
EXPOSE 8000

# Use shell form so PORT env gets read by Python
CMD python -m src.edu_cti.api --host 0.0.0.0
