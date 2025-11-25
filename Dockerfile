# Use the official Apache Airflow image as a base
FROM apache/airflow:2.8.1

# Install PostgreSQL client for psql command
USER root
RUN apt-get update && \
    apt-get install -y postgresql-client libpq-dev build-essential && \
    apt-get clean

# Switch back to the airflow user
USER airflow

# Copy the requirements file and install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the entire project directory into the Airflow home
COPY . /opt/airflow

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow
