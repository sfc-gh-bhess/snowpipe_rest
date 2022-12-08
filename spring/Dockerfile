FROM eclipse-temurin:11

EXPOSE 8080

WORKDIR /app

COPY target/*.jar .

ENTRYPOINT [ "java", "-jar", "SnowpipeRest-0.0.1-SNAPSHOT.jar" ]
