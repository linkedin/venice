package com.linkedin.venice.serializer;

import com.linkedin.venice.schema.vson.VsonAvroDatumWriter;
import org.apache.avro.Schema;


public class VsonAvroGenericSerializer<K> extends AvroGenericSerializer<K> {
  public VsonAvroGenericSerializer(Schema schema) {
    super(new VsonAvroDatumWriter<> (schema));
  }
}
