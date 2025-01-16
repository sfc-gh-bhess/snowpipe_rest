FROM eclipse-temurin:21

EXPOSE 8080

WORKDIR /app

COPY target/*.jar .

ENTRYPOINT [ "java", "-Xms40g", "-Xmx40g", "-XX:+HeapDumpOnOutOfMemoryError", "-jar", "SnowpipeRest-0.0.1-SNAPSHOT.jar", "--server.tomcat.threads.max=10" ]
