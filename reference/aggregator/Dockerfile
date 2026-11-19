FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

FROM eclipse-temurin:21-jre
COPY --from=build /src/target/aggregator.jar /app/aggregator.jar
ENTRYPOINT ["java", "-jar", "/app/aggregator.jar"]
