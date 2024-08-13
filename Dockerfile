FROM apache/airflow:2.4.1

# Update package list, install Java, procps, and Python packages
USER root
RUN apt-get update && \
    apt-get install -y \
        python3-pip \
        procps \
        openjdk-11-jdk-headless && \
    pip3 install \
        boto3 \
        pyspark \
        apache-airflow-providers-apache-spark \
        grpcio-status && \
    # Clean up to reduce image size
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Install Python packages for the airflow user
USER airflow
RUN pip install \
        boto3 \
        pyspark \
        apache-airflow-providers-apache-spark \
        grpcio-status