#!/bin/sh

echo "Waiting for Kafka Connect to start listening on kafka-connect ⏳"

while [ `curl -s -o /dev/null -w "%{http_code}" http://kafka-connect:8083/connectors` -eq 000 ] ; do 
    echo $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://kafka-connect:8083/connectors) " (waiting for 200)"
    sleep 5 
done
nc -vz kafka-connect 8083

echo "Waiting for Cassandra to start listening on cassandradb:9042"
while ! nc -z cassandradb 9042; do
    echo $(date) " Waiting for Cassandra (cassandradb:9042)"
    sleep 3
done
echo "Cassandra is ready!"

echo "\n--\n+> Creating Kafka Connect Cassandra sink"

./create-cassandra-sink.sh 

sleep infinity
