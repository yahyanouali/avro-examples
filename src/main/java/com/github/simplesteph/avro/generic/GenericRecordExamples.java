package com.github.simplesteph.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordExamples {

    public static void main(String[] args) throws IOException {
        // step 0: define schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("""
                {
                     "type": "record",
                     "namespace": "com.example",
                     "name": "Customer",
                     "doc": "Avro Schema for our Customer",
                     "fields": [
                       { "name": "first_name", "type": "string", "doc": "First Name of Customer" },
                       { "name": "last_name", "type": "string", "doc": "Last Name of Customer" },
                       { "name": "age", "type": "int", "doc": "Age at the time of registration" },
                       { "name": "height", "type": "float", "doc": "Height at the time of registration in cm" },
                       { "name": "weight", "type": "float", "doc": "Weight at the time of registration in kg" },
                       { "name": "automated_email", "type": "boolean", "default": true, "doc": "Field indicating if the user is enrolled in marketing emails" }
                     ]
                }
                """);


        // step 1: create generic record
        GenericData.Record customer = new GenericRecordBuilder(schema)
                .set("first_name", "yahya")
                .set("last_name", "nouali")
                .set("age", 35)
                .set("height", 185f)
                .set("weight", 90.5f)
                // .set("automated_email", false)
                .build();

        System.out.println(customer );

        // step 2: write that generic record to a file
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, new File("customer-generic.avro"));
            dataFileWriter.append(customer);
            System.out.println("Successfully write customer and it's schema");
        }

        // step 3: read a generic record from a file
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        File file = new File("customer-generic.avro");
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            // step 4: interpret as a generic record
            customerRead = dataFileReader.next();
        }

        System.out.println("Successfully read customer  " + customerRead);
        System.out.println(customerRead.get("first_name"));

    }
}
