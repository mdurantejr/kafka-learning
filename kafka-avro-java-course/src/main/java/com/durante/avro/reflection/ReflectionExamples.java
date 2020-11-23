package com.durante.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;

public class ReflectionExamples {

    public static void main(final String[] args) {

        // here we use reflection to determine the schema
        final Schema schema = ReflectData.get().getSchema(ReflectedCustomer.class);
        System.out.println("schema = " + schema.toString(true));


        // create a file of ReflectedCustomers
        try {
            System.out.println("Writing customer-reflected.avro");
            final File file = new File("customer-reflected.avro");
            final DatumWriter<ReflectedCustomer> writer = new ReflectDatumWriter<>(ReflectedCustomer.class);
            final DataFileWriter<ReflectedCustomer> out = new DataFileWriter<>(writer)
                    .setCodec(CodecFactory.deflateCodec(9))
                    .create(schema, file);

            out.append(new ReflectedCustomer("Bill", "Clark", "The Rocket"));
            out.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }

        // read from an avro into our Reflected class
        // open a file of ReflectedCustomers
        try {
            System.out.println("Reading customer-reflected.avro");
            final File file = new File("customer-reflected.avro");
            final DatumReader<ReflectedCustomer> reader = new ReflectDatumReader<>(ReflectedCustomer.class);
            final DataFileReader<ReflectedCustomer> in = new DataFileReader<>(file, reader);

            // read ReflectedCustomers from the file & print them as JSON
            for (final ReflectedCustomer reflectedCustomer : in) {
                System.out.println(reflectedCustomer.fullName());
            }
            // close the input file
            in.close();
        } catch (final IOException e) {
            e.printStackTrace();
        }

    }

}