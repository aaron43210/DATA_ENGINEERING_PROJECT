FROM maven:3.9.9-eclipse-temurin-11 AS kafka-connector-build
WORKDIR /build
COPY connectors/pom.xml ./pom.xml
COPY connectors/src ./src
RUN mvn --batch-mode --no-transfer-progress package

FROM flink:1.18.1-scala_2.12-java11
USER root
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
  python3 python3-pip python3-dev gcc libpq-dev curl \
  && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /opt/flink/requirements.txt
ENV PIP_DEFAULT_TIMEOUT=600
# pemja has no arm64 wheel and needs JNI headers to compile; the Flink image is JRE-only.
COPY --from=kafka-connector-build /opt/java/openjdk/include /opt/java/openjdk/include
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt \
 && pip3 install --no-cache-dir --upgrade "dnspython>=2.9.0"
# pyarrow, protobuf and httplib2 stay at the apache-beam 2.48 caps until Flink is upgraded past 1.18.
COPY --from=kafka-connector-build /build/target/sensor-source-envelope-1.0.0.jar /opt/flink/lib/
COPY --from=kafka-connector-build /build/target/runtime-dependencies/ /opt/flink/lib/
COPY jobs/geoplatform_stream_job.py jobs/stream_logic.py jobs/submit_and_monitor.sh /opt/flink/jobs/
ENV PYTHONPATH=/opt/flink/jobs
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints
# Fail the image build if the job graph cannot be constructed.
RUN POSTGRES_PASSWORD=build-check python3 -c "from geoplatform_stream_job import build_job; print(build_job().get_execution_plan())" > /dev/null
USER flink
