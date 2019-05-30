package com.sentinelone.kafkaprotobuff;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import s1.proto.AddressBookProtos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class AddressBookCompressedSerDe implements Serde<AddressBookProtos.AddressBook> {
    private final Serializer<AddressBookProtos.AddressBook> serializer;
    private final Deserializer<AddressBookProtos.AddressBook> deserializer;

    public AddressBookCompressedSerDe() {
        this.serializer = new AddressBookCompressingSerializer();
        this.deserializer = new AddressBookDecompressingDeserializer();
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

    static class AddressBookCompressingSerializer implements Serializer<AddressBookProtos.AddressBook> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }


        @Override
        public void close() {
        }

        @Override
        public byte[] serialize(String topic, AddressBookProtos.AddressBook addressBook) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (final DeflaterOutputStream ofs = new DeflaterOutputStream(baos)) {
                ofs.write(addressBook.toByteArray());
            } catch (final IOException e) {
                System.err.println("Failed serializing KafkaSerializableOutput");
            }
            return baos.toByteArray();
        }
    }

    static class AddressBookDecompressingDeserializer implements Deserializer<AddressBookProtos.AddressBook> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {

        }

        @Override
        public void close() {

        }

        @Override
        public AddressBookProtos.AddressBook deserialize(String topic, byte[] data) {
            try (final InflaterInputStream ifs = new InflaterInputStream(new ByteArrayInputStream(data))) {
                return AddressBookProtos.AddressBook.parseFrom(ifs);
            } catch (final IOException e) {
                throw new SerializationException("Failed deserializing from Protobuf message", e);
            }
        }

    }
}
