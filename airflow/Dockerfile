FROM apache/airflow:2.9.1-python3.9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-mysql-client \
    postgresql-client \
    curl \
    wget \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Create directories for integration
RUN mkdir -p /opt/spark/jars
RUN mkdir -p /opt/airflow/scripts

# Switch to airflow user for pip installations
USER airflow

# Install required packages for your core setup
RUN pip install --no-cache-dir \
    minio \
    kafka-python \
    psycopg2-binary \
    mysql-connector-python \
    pandas \
    pyarrow \
    python-dotenv \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    apache-airflow-providers-mysql \
    apache-airflow-providers-amazon

# Set environment variables (SIMPLIFIED - remove problematic PATH)
ENV SPARK_HOME=/opt/spark
