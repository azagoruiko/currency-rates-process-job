#!/usr/bin/env bash
VER=$1
BUILD=$2
if [[ ! -z $BUILD ]]; then
    mvn clean install
fi
docker build "--tag=10.8.0.5:9999/docker/rates-update:${VER}" ./
docker push "10.8.0.5:9999/docker/rates-update:${VER}"
export NOMAD_ADDR=http://10.8.0.1:4646
#cp target/sparkjob-jar-with-dependencies.jar /opt/spark/examples/jars/sparkjob.jar
#docker run -it --network host "10.8.0.5:9999/docker/rates-update:${VER}"  bash /app/run.sh "${VER}"
nomad job run update-rates.nomad
#bash bin/run.sh "${VER}"
