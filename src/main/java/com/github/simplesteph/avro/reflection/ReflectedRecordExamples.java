package com.github.simplesteph.avro.reflection;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class ReflectedRecordExamples {

    public static void main(String[] args) {

        Schema schema = ReflectData.get().getSchema(Student.class);
        System.out.println("the generated schema is " + schema.toString(true));
    }
}
