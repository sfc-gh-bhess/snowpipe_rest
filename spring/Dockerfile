FROM eclipse-temurin:21

EXPOSE 8080

WORKDIR /app

COPY target/*.jar .

ENTRYPOINT [ "java", "-Xms8g", "-Xmx8g", "-jar", "SnowpipeRest-0.0.1-SNAPSHOT.jar" ]
