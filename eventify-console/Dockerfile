# The Eventify Console: one jar with the server and the UI.
# Build from the repository root: docker build -f eventify-console/Dockerfile -t eventify-console .

# 1. The jar; the Maven build also builds the UI (frontend/) into it. -am builds the protocol module it depends on.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
RUN mvn --batch-mode --no-transfer-progress -pl eventify-console -am -DskipTests package

# 2. The image
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/eventify-console/target/eventify-console-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
