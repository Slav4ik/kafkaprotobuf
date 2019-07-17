package com.example.kafkaprotobuf;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import s1.proto.AddressBookProtos;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaSaslScramClient {

    /**
     * Example program how to configure your kafka client to authenticate with SASL_SCRAM
     * All lines with comments should be updated respectively
     *
     * @param args - ignore
     */
    public static void main(String[] args) {
        Properties props = new Properties();


        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.brokers.urls.com:9092"); //Kafka brokers hostnames
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test"); // Consumer group ID, should be shared between all consumers in the same group
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AddressBookCompressedSerDe.class.getName());

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "TRUSTORE_PATH"); //Location of Kafka CA Trustore
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "TRUSTORE_PASS"); //Password to open and use CA Truststore
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        String kafkaUsername = ""; //Kafka topic username
        String kafkaPassword = ""; //Kafka topic password
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, kafkaUsername, kafkaPassword);
        props.put("sasl.jaas.config", jaasCfg);

        KafkaConsumer<String, AddressBookProtos.AddressBook> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("your-kafka-topic")); //Kafka topic name
        while (true) {
            ConsumerRecords<String, AddressBookProtos.AddressBook> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, AddressBookProtos.AddressBook> addressBookRecord : records) {
                final AddressBookProtos.AddressBook addressBook = addressBookRecord.value();
                System.out.printf("address book size = %d%n", addressBook.getPeopleCount());
            }
        }
    }
}
