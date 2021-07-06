FROM maven:3.6.1-jdk-11 as build

COPY  . /home/maven/src

WORKDIR /home/maven/src

RUN mvn clean package

FROM openjdk:11-jdk-slim

RUN mkdir /app

COPY --from=build /home/maven/src/target/totalcover-1.0.0-jar-with-dependencies.jar /app/target/totalcover-1.0.0-jar-with-dependencies.jar
COPY start.sh app/start.sh
WORKDIR /app
RUN ls
CMD ["./start.sh"]
