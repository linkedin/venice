package com.linkedin.venice.serializer;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

public class AvroSpecificDeserializer<T extends SpecificRecord> extends AvroGenericDeserializer<T> {
  public AvroSpecificDeserializer(Schema writer, Class<T> c) {
    super(new SpecificDatumReader<>(writer, SpecificData.get().getSchema(c)));
  }
}
