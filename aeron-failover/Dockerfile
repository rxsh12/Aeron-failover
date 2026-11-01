FROM maven:3.9-eclipse-temurin-21 AS java-build
WORKDIR /workspace/aeron-failover
COPY aeron-failover/.mvn/ .mvn/
COPY aeron-failover/mvnw aeron-failover/pom.xml ./
COPY aeron-failover/src/ src/
RUN ./mvnw -DskipTests package

FROM gcc:13-bookworm AS publisher-build
WORKDIR /workspace
COPY shm-udp/publisher.c publisher.c
RUN cc -O3 -std=c11 -Wall -Wextra -Wpedantic -Werror -o publisher publisher.c

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=java-build /workspace/aeron-failover/target/aeron-failover-1.0.0.jar /app/target/aeron-failover-1.0.0.jar
COPY aeron-failover/run.sh /app/run.sh
COPY --from=publisher-build /workspace/publisher /app/publisher
RUN mkdir -p /app/data /data && chmod +x /app/run.sh /app/publisher
ENV AERON_DEMO_CONFIG=/app/config.properties
ENTRYPOINT ["/app/run.sh"]
