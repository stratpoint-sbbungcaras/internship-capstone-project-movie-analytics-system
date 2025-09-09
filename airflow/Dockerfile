# Use a base image that already has Python and is suitable for Airflow.
FROM apache/airflow:2.8.1

# Switch to the root user to perform system-level changes.
USER root

# Install a compatible Java runtime environment (JRE)
# Using a more robust package name for Debian
RUN apt-get update \
    && apt-get install -y openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH=$PATH:$JAVA_HOME/bin

# Set permissions for the Airflow user
RUN chmod -R 777 /opt/airflow/

# Switch back to the non-privileged airflow user for security.
USER airflow

# Install PySpark and other dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
