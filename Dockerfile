FROM 10.8.0.5:9999/docker/spark-nomad-aws-client:test1

#WORKDIR /app
#RUN wget https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-22.3.0/graalvm-ce-java11-linux-amd64-22.3.0.tar.gz

#WORKDIR /opt/graalvm
#RUN tar -xzf /app/graalvm-ce-java11-linux-amd64-22.3.0.tar.gz

#ENV JAVA_HOME=/opt/graalvm/graalvm-ce-java11-22.3.0

WORKDIR /app
RUN wget --no-check-certificate https://dlcdn.apache.org/hive/hive-1.2.2/apache-hive-1.2.2-bin.tar.gz
WORKDIR /opt
RUN tar -xzf /app/apache-hive-1.2.2-bin.tar.gz
#RUN cp /opt/apache-hive-3.1.3-bin/lib/hive*.jar /opt/spark/jars/


#COPY statements-ingest-spark.json  /app/statements-ingest-spark.json
COPY bin/run.sh  /app/run.sh
COPY bin/run_local.sh  /app/run_local.sh

#ENV PATH="${PATH}:/opt/spark/bin:/opt/graalvm/graalvm-ce-java11-22.3.0/bin:/opt/apache-hive-3.1.3-bin/bin"
ENV PATH="${PATH}:/opt/spark/bin:/opt/apache-hive-1.2.2-bin/bin"

WORKDIR /app
COPY target/sparkjob-jar-with-dependencies.jar /app/sparkjob.jar
