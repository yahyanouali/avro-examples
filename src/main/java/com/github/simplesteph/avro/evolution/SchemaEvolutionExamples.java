package com.github.simplesteph.avro.evolution;

import com.example.Customer;
import com.example.CustomerV1;
import com.example.CustomerV2;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SchemaEvolutionExamples {

    public static void main(String[] args) throws IOException {
        // 1 Create customers
        CustomerV1 customerV1 = CustomerV1.newBuilder().setFirstName("yahya")
                .setLastName("nouali")
                .setAge(35)
                .setWeight(90)
                .setHeight(185)
                .setAutomatedEmail(false)
                .build();


        CustomerV2 customerV2 = CustomerV2.newBuilder().setFirstName("yahya")
                .setLastName("nouali")
                .setAge(35)
                .setWeight(90)
                .setHeight(185)
                .setPhoneNumber("0777777")
                .build();

        // 2 write customerV1 with schema V1
        DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>();
        File customerV1schemaV1File = new File("customer-v1-schema-v1.avro");
        try (DataFileWriter<CustomerV1> fileWriter = new DataFileWriter<>(datumWriter)) {
            fileWriter.create(customerV1.getSchema(), customerV1schemaV1File);
            fileWriter.append(customerV1);
            System.out.println("successfully wrote consumer with schema v1 " + customerV1);
        }

        // 3 read customerV1 with new schema V2
        DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
        try (DataFileReader<CustomerV2> fileReader = new DataFileReader<>(customerV1schemaV1File, datumReader)) {
            CustomerV2 readCustomerV2 = fileReader.next();
            System.out.println("read old customer with new schema v2  " + readCustomerV2);
        }

        // 4 write customerV2 with schema V2
        DatumWriter<CustomerV2> datumWriter2 = new SpecificDatumWriter<>();
        File customerV2schemaV2File = new File("customer-v2-schema-v2.avro");
        try (DataFileWriter<CustomerV2> fileWriter = new DataFileWriter<>(datumWriter2)) {
            fileWriter.create(customerV2.getSchema(), customerV2schemaV2File);
            fileWriter.append(customerV2);
            System.out.println("successfully wrote consumer with new schema v2 " + customerV1);
        }

        // 5 read customerV2 with old schema V1
        DatumReader<CustomerV1> datumReader2 = new SpecificDatumReader<>(CustomerV1.class);
        try (DataFileReader<CustomerV1> fileReader = new DataFileReader<>(customerV2schemaV2File, datumReader2)) {
            CustomerV1 readCustomerV1 = fileReader.next();
            System.out.println("read customer with old schema v1  " + readCustomerV1);
        }


    }

}
