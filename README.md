## Vet API Demo

A demonstration application written in java using kafka streams and avro.

### Dependencies

- docker and docker-compose installed.
- Java 11 JDK

### Running

first start the docker environment

`docker-compose up -d`

next create the necessary topics

`./create-topics.sh`

finally start the app

`./gradlew bootRun`

### Demo

To initiate data flow you can use the below curl command which mimicks
polling an api endpoint for updates and publishing them into the broker.

```
curl localhost:8080/test
```

To create a continuous stream of random updates you can put that curl
command in a loop like so.

```
while true; do curl localhost:8080/test; sleep 5; done
```

You can use Control Center to view the topics and consumers at
http://localhost:9021.

