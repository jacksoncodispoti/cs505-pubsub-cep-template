#!/bin/sh
mvn clean package
cd target
java -Djava.net.preferIPv4Stack=true -jar cs505-pubsub-cep-template-1.0-SNAPSHOT.jar
