FROM maven:3.9.9-eclipse-temurin-11 AS kafka-connector-build
WORKDIR /build
COPY connectors/pom.xml ./pom.xml
COPY connectors/src ./src
RUN mvn --batch-mode --no-transfer-progress package

FROM flink:1.18.1-scala_2.12-java11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-dev gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt
COPY --from=kafka-connector-build /build/target/sensor-source-envelope-1.0.0.jar /opt/flink/lib/
COPY --from=kafka-connector-build /build/target/runtime-dependencies/ /opt/flink/lib/
COPY jobs/ /opt/flink/jobs/
ENV PYTHONPATH=/opt/flink/jobs
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
RUN POSTGRES_PASSWORD=build-check python3 -c "from sensor_flink_job import build_job; print(build_job().get_execution_plan())"
USER flink
