FROM 10.8.0.5:5000/spark-s3:0.0.3

WORKDIR /app
COPY target/sparkjob-jar-with-dependencies.jar /app/rates-trades.jar

COPY bin/run.sh  /app/run.sh

ENV PATH="${PATH}:/opt/spark/bin"
