FROM maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

FROM flink:1.19.1-scala_2.12-java11
# named volume inherits this ownership on first mount, so checkpoints are writable
RUN mkdir -p /opt/flink/usrlib /flink-data && chown -R flink:flink /flink-data
COPY --from=build /src/target/feature-job.jar /opt/flink/usrlib/feature-job.jar
COPY --chmod=755 start-job.sh /opt/flink/bin/start-job.sh
