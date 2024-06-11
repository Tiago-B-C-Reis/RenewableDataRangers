FROM quay.io/astronomer/astro-runtime:11.4.0

# Install necessary Kerberos libraries and tools
USER root
RUN apt-get update && apt-get install -y \
    krb5-user \
    libkrb5-dev \
    build-essential  # Install build-essential for development tools

# Switch back to the airflow user
USER astro

# Install additional packages
COPY requirements.txt .
RUN pip install -r requirements.txt

