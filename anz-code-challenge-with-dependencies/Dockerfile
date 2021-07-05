### BUILDER IMAGE ###
FROM maven:3-jdk-11-slim as builder

COPY pom.xml /build/

RUN mvn --file build/pom.xml --batch-mode package

COPY src /build/src/

RUN mvn --file build/pom.xml --batch-mode package -DskipTests


### PRODUCTION IMAGE ###
FROM openjdk:11-jre-slim

COPY --from=builder build/target/anz-code-challenge-with-dependencies-0.0.1-SNAPSHOT.jar app/app.jar

WORKDIR /app

CMD ["java", "-cp", "app.jar", "com.anz.Driver"]
