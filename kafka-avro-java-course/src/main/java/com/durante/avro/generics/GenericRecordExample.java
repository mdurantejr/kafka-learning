package com.durante.avro.generics;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;

import java.io.File;
import java.io.IOException;

public class GenericRecordExample {

    public static void main(final String[] args) {
        // 0: define schema
        final Schema.Parser parser = new Schema.Parser();
        final Schema schema = parser.parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");

        // 1: create a generic record
        final GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Doe");
        customerBuilder.set("age", 25);
        customerBuilder.set("height", 170F);
        customerBuilder.set("weight", 80.5F);
        customerBuilder.set("automated_email", false);
        final GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

        // 2: write the generic record to a file
        final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(customer);
            System.out.println("Written cusomter-generic.avro");
        } catch (final IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // 3: read a generic record from a file
        final File file = new File("customer-generic.avro");
        final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        try (final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            final GenericRecord customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            // 4: interpret as a generic record
            System.out.println(customerRead.toString());
            // get data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));
            // read non-existing field - prints null
            System.out.println("Non existing field: " + customerRead.get("not_here"));
        } catch (final IOException e) {
            e.printStackTrace();
        }

    }

}