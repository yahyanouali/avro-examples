package com.github.simplesteph.avro.specific;

import com.example.Customer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExamples {

    public static void main(String[] args) throws IOException {
        // step 1: create specific record
        Customer customer = Customer.newBuilder()
                .setFirstName("Yahya")
                .setLastName("NOUALI")
                .setAge(35)
                .setWeight(91)
                .setHeight(185)
                .setAutomatedEmail(false)
                .build();

        System.out.println(customer);
        System.out.println(customer.getLastName());

        // step 2: write to file (record + schema)
        SpecificDatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        File avroFile = new File("customer-specific.avro");
        try (DataFileWriter<Customer> fileWriter = new DataFileWriter<>(datumWriter)) {
            fileWriter.create(customer.getSchema(), avroFile);
            fileWriter.append(customer);
        }

        // step 3: read from file (record only)
        SpecificDatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        Customer customerRead;
        try (DataFileReader<Customer> fileReader = new DataFileReader<>(avroFile, datumReader)) {
            // step 4: interpret record as Customer
            customerRead = fileReader.next();
        }
        System.out.println("successfully read customer");
        System.out.println(customerRead);

    }
}
