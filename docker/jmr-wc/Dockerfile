FROM eclipse-temurin:25-jre

WORKDIR /opt/jmr

COPY my-mapreduce-job/target/my-mapreduce-job-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/my-mapreduce-job.jar
COPY docker/jmr-runtime/submitter-log4j2.xml /opt/jmr/config/submitter-log4j2.xml

RUN mkdir -p /data /outputs /opt/jmr/config

ENTRYPOINT ["java", "-Dlog4j.configurationFile=file:/opt/jmr/config/submitter-log4j2.xml", "-Dlog4j2.statusLoggerLevel=WARN", "-cp", "/opt/jmr/my-mapreduce-job.jar", "com.example.MyWcJob"]
