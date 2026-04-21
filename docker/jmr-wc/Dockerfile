FROM eclipse-temurin:25-jre

WORKDIR /opt/jmr

RUN mkdir -p /data /outputs /opt/jmr/config

COPY target/my-mapreduce-job-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/jmr/my-mapreduce-job.jar
COPY src/main/resources/log4j2.xml /opt/jmr/config/log4j2.xml

ENTRYPOINT ["java", "-Dlog4j.configurationFile=file:/opt/jmr/config/log4j2.xml", "-Dlog4j2.statusLoggerLevel=WARN", "-cp", "/opt/jmr/my-mapreduce-job.jar", "com.example.MyWcJob"]
