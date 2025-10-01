FROM python:3.9-slim

# Install only essential tools
RUN apt-get update && \
    apt-get install -y \
      curl \
      wget \
      netcat-openbsd \
      less \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for data engineering
RUN pip install --no-cache-dir \
      kafka-python==2.0.2 \
      minio==7.2.16 \
      mysql-connector-python==8.0.33 \
      pandas==2.0.3 \
      requests

# Create working directory
RUN mkdir -p /workspace && chmod 777 /workspace

WORKDIR /workspace

# stay running
CMD ["bash", "-c", "echo '🛠 tools container ready'; tail -f /dev/null"]
