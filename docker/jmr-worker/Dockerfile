FROM eclipse-temurin:25-jre

WORKDIR /opt/jmr

COPY mapreduce-parent/jmr-worker/target/jmr-worker-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/jmr-worker.jar

RUN mkdir -p /var/lib/jmr

ENTRYPOINT ["java", "-cp", "/opt/jmr/jmr-worker.jar", "it.jmr.worker.StaticWorkerLauncher"]
