package com.durante.avro.specific;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExamples {

    public static void main(final String[] args) {
        // 1: create specific record
        final Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("John");
        customerBuilder.setLastName("Joe");
        customerBuilder.setHeight(175.5F);
        customerBuilder.setWeight(80.5F);
        customerBuilder.setAutomatedEmail(false);
        final Customer customer = customerBuilder.build();
        System.out.println(customer);
        // 2: write to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (final DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("Successfully wrote customer-specific.avro");
        } catch (final IOException e) {
            e.printStackTrace();
        }

        // 3: read from file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try {
            System.out.println("Reading our specific record");
            final DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                // 4: interpret
                final GenericRecord customerRead = dataFileReader.next();
                System.out.println(customerRead.toString());
                System.out.println("First name: " + customerRead.get("first_name"));
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

    }

}
