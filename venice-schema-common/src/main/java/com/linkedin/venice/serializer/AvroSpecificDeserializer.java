package com.linkedin.venice.serializer;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

public class AvroSpecificDeserializer<T extends SpecificRecord> extends AvroGenericDeserializer<T> {
  public AvroSpecificDeserializer(Schema writer, Class<T> c, IterableImpl iterableImpl) {
    super(new SpecificDatumReader<>(writer, SpecificData.get().getSchema(c)), iterableImpl);
  }

  public AvroSpecificDeserializer(DatumReader<T> datumReader, IterableImpl iterableImpl) {
    super(datumReader, iterableImpl);
  }
}
