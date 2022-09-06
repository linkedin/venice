package com.linkedin.venice.serializer;

import com.linkedin.venice.schema.vson.VsonAvroDatumReader;
import org.apache.avro.Schema;


public class VsonAvroGenericDeserializer<K> extends AvroGenericDeserializer<K> {
  public VsonAvroGenericDeserializer(Schema writer, Schema reader) {
    super(new VsonAvroDatumReader<>(writer, reader));
  }
}
