package com.sentinelone.kafkaprotobuff;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import s1.proto.AddressBookProtos;

import java.util.Map;

public class AddressBookSerDe implements Serde<AddressBookProtos.AddressBook> {
    private final Serializer<AddressBookProtos.AddressBook> serializer;
    private final Deserializer<AddressBookProtos.AddressBook> deserializer;

    public AddressBookSerDe() {
        this.serializer = new AddressBookSerializer();
        this.deserializer = new AddressBookDeserializer();
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<AddressBookProtos.AddressBook> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<AddressBookProtos.AddressBook> deserializer() {
        return deserializer;
    }

    static class AddressBookSerializer implements Serializer<AddressBookProtos.AddressBook> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }


        @Override
        public void close() {
        }

        @Override
        public byte[] serialize(String topic, AddressBookProtos.AddressBook addressBook) {
            return addressBook.toByteArray();
        }
    }

    static class AddressBookDeserializer implements Deserializer<AddressBookProtos.AddressBook> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public AddressBookProtos.AddressBook deserialize(String topic, byte[] data) {
            try {
                return AddressBookProtos.AddressBook.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                throw new SerializationException("Error deserializing from Protobuf message", e);
            }
        }

    }
}
