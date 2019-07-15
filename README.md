# kafkaprotobuf
A simple example of Kafka Client consuming Protobuf messages.

Run `mvn clean compile` to generate java classes for the protobuf schemas.

To use unencrypted kafka cluster use the client like [here](https://github.com/Slav4ik/kafkaprotobuff/blob/master/src/main/java/com/sentinelone/kafkaprotobuff/KafkaClient.java)

To use SSL secured kafka cluster with SCRAM authentication use the client like [here](https://github.com/Slav4ik/kafkaprotobuff/blob/master/src/main/java/com/sentinelone/kafkaprotobuff/KafkaSaslScramClient.java)
  
