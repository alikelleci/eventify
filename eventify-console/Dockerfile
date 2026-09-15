# The Eventify Console: the Spring Boot server with the Angular UI.
# Build from the repository root: docker build -f eventify-console/Dockerfile -t eventify-console .

# 1. The UI
FROM node:22-alpine AS ui
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build:console

# 2. The server; -am also builds the protocol module it depends on
FROM maven:3.9-eclipse-temurin-21 AS server
WORKDIR /build
COPY . .
RUN mvn --batch-mode --no-transfer-progress -pl eventify-console -am -DskipTests package

# 3. The image
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=server /build/eventify-console/target/eventify-console-*.jar app.jar
COPY --from=ui /frontend/dist/console/browser ui/

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
