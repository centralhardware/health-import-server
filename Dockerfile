FROM gradle:8-jdk21 AS builder
WORKDIR /app
COPY . .
RUN gradle installDist --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/build/install/health-import-kotlin/ /app/
EXPOSE 8080
ENTRYPOINT ["bin/health-import-kotlin"]

