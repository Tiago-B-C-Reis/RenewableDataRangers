FROM quay.io/astronomer/astro-runtime:11.5.0

# Use the standard Airflow image as the base image
FROM apache/airflow:2.9.2

# Switch to root user to install apt packages
USER root

# Install build-essential, Kerberos libraries, and clean up after installation
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           build-essential \
           openjdk-11-jdk-headless \
           gcc \
           heimdal-dev \
           krb5-user \
           libkrb5-dev \
           libkrb5-3 \
           libkrb5support0 \
           libk5crypto3 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Ensure krb5-config has the correct permissions
RUN chmod +x /usr/bin/krb5-config

# Set JAVA_HOME environment variable for pyspark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to airflow user
USER airflow

# Add requirements.txt and install the packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Ensure that pyspark and other dependencies are installed correctly
RUN pip install pyspark==3.1.2