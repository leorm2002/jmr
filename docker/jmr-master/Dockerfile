FROM eclipse-temurin:25-jre

WORKDIR /opt/jmr

COPY mapreduce-parent/jmr-master/target/jmr-master-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/jmr-master.jar

RUN mkdir -p /var/lib/jmr

ENTRYPOINT ["java", "-cp", "/opt/jmr/jmr-master.jar", "it.jmr.master.StaticMasterLauncher"]
