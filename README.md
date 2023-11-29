# Import dpa-digitalwires via wireQ-API

This repository shows best practices of how to import the dpa-digitalwires format via wireQ-API and store the received articles to the local file system.

In general there are two approaches:

1. Fast and efficient retrieval of all new messages via POST request `dequeue_entries.json`
2. Fast and efficient retrieval of all new messages via GET and DELETE request

This example uses the second approach and sends the message to an ActiveMQ message broker before sending the delete request to the wireQ-API.

## Requirements

    - Java 21+
    - Docker

## Starting

The easiest way to run this demo is by using Docker.

```
    export BASE_URL=https://...
    ./mvnw verify
```

This will create and start two containers. First an ActiveMQ server is started and then a container, which runs the 
Connector.

Alternatively you can change the Connector, so it uses a remote ActiveMQ server.
That way you can run it on the host machine without Docker. (**Note**: Before create an image you might change the registry inside the `pom.xml` from the Connector project to your own needs.)

After the build has finished, you can view the received data by browsing to `localhost:8161`. Enter "artemis" as both user and password and search for the `dpa.demo` queue.

## Cleaning up
```
    ./mvnw clean
```

This will both stop and remove the containers.