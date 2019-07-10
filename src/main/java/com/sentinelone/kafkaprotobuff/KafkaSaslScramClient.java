package com.sentinelone.kafkaprotobuff;

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

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.brokers.urls.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AddressBookCompressedSerDe.class.getName());

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "TRUSTORE_PATH");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "TRUSTORE_PASS");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "KEYSTORE_PATH");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "KEYSTORE_PASS");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        String kafkaUsername = "";
        String kafkaPassword = "";
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, kafkaUsername, kafkaPassword);
        props.put("sasl.jaas.config", jaasCfg);

        KafkaConsumer<String, AddressBookProtos.AddressBook> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("your-kafka-topic"));
        while (true) {
            ConsumerRecords<String, AddressBookProtos.AddressBook> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, AddressBookProtos.AddressBook> addressBookRecord : records) {
                final AddressBookProtos.AddressBook addressBook = addressBookRecord.value();
                System.out.printf("address book size = %d%n", addressBook.getPeopleCount());
            }
        }
    }
}
