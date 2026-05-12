# Default monolith image: pre-built `./gradlew bootJar`.
# Build: `docker build -f docker/Dockerfile --build-arg JAR_FILE=build/libs/oms-0.1.0-SNAPSHOT.jar -t oms:local .`

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

ARG JAR_FILE=build/libs/oms-0.1.0-SNAPSHOT.jar
COPY ${JAR_FILE} /app/oms.jar

ENV JAVA_TOOL_OPTIONS="\
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED"

EXPOSE 8088

ENTRYPOINT ["java", "-jar", "/app/oms.jar"]
