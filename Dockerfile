# Multi-stage build for StarRocks Kafka Connector
# Stage 1: Build the connector
FROM maven:3.9.11 AS builder

# Set working directory
WORKDIR /build

# Copy Maven files first for better layer caching
COPY pom.xml .
COPY src/ src/

# Build the project and create the distribution
RUN mvn clean package assembly:single -DskipTests && \
    ls -la target/

# Stage 2: Runtime image with Kafka Connect
FROM confluentinc/cp-kafka-connect:7.9.4

ENV STARROCKS_CONNECTOR_NAME=starrocks-kafka-connector

# Switch to root for installation
USER root

# Install curl for health checks and clean up in same layer
RUN yum update -y && \
    yum install -y curl && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    mkdir -p /usr/share/confluent-hub-components/${STARROCKS_CONNECTOR_NAME}

# Copy built connector from builder stage
COPY --from=builder /build/target/starrocks-connector-for-kafka-*-package/share/java/s${STARROCKS_CONNECTOR_NAME}/ \
     /usr/share/confluent-hub-components/${STARROCKS_CONNECTOR_NAME}/

ADD https://github.com/aws/aws-msk-iam-auth/releases/download/v2.3.4/aws-msk-iam-auth-2.3.4-all.jar /usr/share/confluent-hub-components/aws-msk-iam-auth/
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/jmx_exporter

# Switch back to appuser
USER appuser

# Set plugin path to include our connector
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"
ENV CLASSPATH=/usr/share/confluent-hub-components/*:

ENV KAFKA_HEAP_OPTS="-Xms1G -Xmx4G"
ENV KAFKA_OPTS="-Djavax.net.debug=ssl -Dlog4j.configuration=file:/etc/kafka/log4j.properties"
ENV KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.port=9010"
ENV JAVA_TOOL_OPTIONS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=9091:/etc/jmx-exporter/config.yaml"

ENV JMX_PORT=9010


# Expose Kafka Connect port
EXPOSE 8083
# Expose JMX port
EXPOSE 9010
# Expose Prometheus metrics port
EXPOSE 9091

# Health check to verify connector is loaded
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD curl -f http://localhost:8083/connector-plugins | grep -q "StarRocksSinkConnector" || exit 1

# Default command (inherited from base image)

