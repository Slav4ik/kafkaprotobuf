package com.example.kafkaprotobuf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import s1.proto.AddressBookProtos;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaClient {

    /**
     * Example program how to configure your kafka client
     *
     * @param args - ignore
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");

        props.setProperty("key.deserializer", StringDeserializer.class.getName());

        // props.setProperty("value.deserializer", AddressBookSerDe.class.getName());
        // if expected package is compressed - user compressing variant
        props.setProperty("value.deserializer", AddressBookCompressedSerDe.class.getName());

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
