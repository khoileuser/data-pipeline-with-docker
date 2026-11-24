#!/bin/sh


echo "Starting Weather Sink"
curl -s \
     -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
  "name": "weathersink",
  "config": {
    "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",  
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":"false",
    "tasks.max": "10",
    "topics": "weather",
    "contactPoints": "cassandradb",
    "loadBalancing.localDc": "datacenter1",
    "topic.weather.kafkapipeline.weatherreport.mapping": "location=value.location, forecastdate=value.report_time, description=value.description, temp=value.temp, feels_like=value.feels_like, temp_min=value.temp_min, temp_max=value.temp_max, pressure=value.pressure, humidity=value.humidity, wind=value.wind, sunrise=value.sunrise, sunset=value.sunset",
    "topic.weather.kafkapipeline.weatherreport.consistencyLevel": "LOCAL_QUORUM"
  }
}'
echo "Done."

echo "Starting Faker Sink"
curl -s \
     -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
  "name": "fakersink",
  "config": {
    "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",  
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":"false",
    "tasks.max": "10",
    "topics": "faker",
    "contactPoints": "cassandradb",
    "loadBalancing.localDc": "datacenter1",
    "topic.faker.kafkapipeline.fakerdata.mapping": "user_id=value.user_id, username=value.username, name=value.name, email=value.email, phone=value.phone, birthdate=value.birthdate, job=value.job, company=value.company, address=value.address, city=value.city, postcode=value.postcode, country=value.country, passport_number=value.passport_number, credit_card_provider=value.credit_card_provider, credit_card_number=value.credit_card_number, credit_card_expire=value.credit_card_expire, ip_address=value.ip_address, user_agent=value.user_agent, registered_at=value.registered_at",
    "topic.faker.kafkapipeline.fakerdata.consistencyLevel": "LOCAL_QUORUM"
  }
}'
echo "Done."

echo "Starting Earthquake Sink"
curl -s \
     -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
  "name": "earthquakesink",
  "config": {
    "connector.class": "com.datastax.oss.kafka.sink.CassandraSinkConnector",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable":"false",
    "tasks.max": "10",
    "topics": "earthquake",
    "contactPoints": "cassandradb",
    "loadBalancing.localDc": "datacenter1",
    "topic.earthquake.kafkapipeline.earthquakedata.mapping": "id=value.id, place=value.place, magnitude=value.magnitude, mag_type=value.mag_type, event_time=value.event_time, updated_time=value.updated_time, latitude=value.latitude, longitude=value.longitude, depth_km=value.depth_km, significance=value.significance, felt_reports=value.felt_reports, alert_level=value.alert_level, tsunami=value.tsunami, status=value.status, event_type=value.event_type, network=value.network, url=value.url",
    "topic.earthquake.kafkapipeline.earthquakedata.consistencyLevel": "LOCAL_QUORUM"
  }
}'
echo "Done."