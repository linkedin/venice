package com.linkedin.venice.serializer;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;


public class ComputableAvroSpecificDeserializer<T extends SpecificRecord> extends AvroSpecificDeserializer<T> {
  public ComputableAvroSpecificDeserializer(Schema writer, Class<T> c, IterableImpl iterableImpl) {
    super(new ComputableSpecificDatumReader<>(writer, SpecificData.get().getSchema(c)), iterableImpl);
  }
}
