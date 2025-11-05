# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install git (needed for cloning)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Clone private GitHub repo into /config
ARG GITHUB_TOKEN
ADD https://api.github.com/repos/voteagora/tenants/git/refs/heads/master /tmp/cache-bust.json
RUN git clone https://${GITHUB_TOKEN}@github.com/voteagora/tenants.git /config && \
    cd /config && \
    echo "Cloned commit SHA: $(git rev-parse HEAD)"

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
