# The Eventify Console: one jar with the server and the UI.
# Build from the repository root: docker build -f eventify-console-server/Dockerfile -t eventify-console .

# 1. The jar; the Maven build also builds the UI (frontend/) into it. -am also builds the modules it depends on: the
#    protocol, and the plugin and core its tests use.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY . .
RUN mvn --batch-mode --no-transfer-progress -pl eventify-console-server -am -DskipTests package

# 2. The image
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/eventify-console-server/target/eventify-console-server-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
