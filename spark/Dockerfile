FROM bitnami/spark:3.5.0

# Set workdirectory for COPY
WORKDIR /opt/bitnami/spark

#a tool needed for Spark to work with Python.
RUN pip install py4j
# Copy spark scripts and constants script required.
COPY ./src/spark_pgsql/spark_streaming.py ./spark_streaming.py
COPY ./src/constants.py ./src/constants.py

# Auto resolve to internal IP address use by host. For PROD case, please configure to Actual IP address
ENV POSTGRES_DOCKER_USER=host.docker.internal