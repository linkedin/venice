package com.linkedin.venice.schema.avro;

import com.linkedin.venice.serializer.AvroGenericDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;


public class ComputableAvroGenericDeserializer<T> extends AvroGenericDeserializer<T> {
  public ComputableAvroGenericDeserializer(Schema writer, Class<T> c, IterableImpl iterableImpl) {
    super(new ComputableGenericDatumReader<>(writer, SpecificData.get().getSchema(c)), iterableImpl);
  }

  public ComputableAvroGenericDeserializer(Schema writer, Schema reader) {
    super(new ComputableGenericDatumReader(writer, reader));
  }
}
