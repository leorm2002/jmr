FROM eclipse-temurin:21-jre

WORKDIR /opt/jmr

COPY mapreduce-parent/jmr-master/target/jmr-master-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/jmr-master.jar
COPY mapreduce-parent/jmr-worker/target/jmr-worker-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/jmr-worker.jar

RUN mkdir -p /var/lib/jmr

ENTRYPOINT ["java"]
