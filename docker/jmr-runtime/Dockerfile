FROM eclipse-temurin:21-jre

WORKDIR /opt/jmr

COPY mapreduce-parent/jmr-master/target/jmr-master-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/jmr-master.jar
COPY mapreduce-parent/jmr-worker/target/jmr-worker-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/jmr-worker.jar
COPY my-mapreduce-job/target/my-mapreduce-job-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/my-mapreduce-job.jar
COPY docker/jmr-runtime/submitter-log4j2.xml /opt/jmr/config/submitter-log4j2.xml
COPY data/serialized_data /opt/jmr/data/serialized_data

RUN mkdir -p /var/lib/jmr /outputs /opt/jmr/config

ENTRYPOINT ["java"]
