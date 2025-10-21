# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY cpls/ ./cpls/
COPY .env.example .env.example

# Expose the server port (default is 8001)
EXPOSE 8001

# Set environment variables with defaults
ENV PYTHONUNBUFFERED=1 \
    SERVER_HOST=0.0.0.0 \
    SERVER_PORT=8001

# Run the server
CMD ["python", "-m", "uvicorn", "cpls.server:app", "--host", "0.0.0.0", "--port", "8001"]
