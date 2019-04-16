package com.linkedin.venice.serializer;

import com.linkedin.avro.fastserde.FastGenericDatumWriter;
import com.linkedin.avro.fastserde.FastSerdeCache;
import org.apache.avro.Schema;


/**
 * This class is leveraging linkedin-avro-fastserde-impl to speed up serialization,
 * and so far it doesn't work for Avro-1.4, and it will fail back to regular Avro.
 * @param <K>
 */
public class FastAvroGenericSerializer<K> extends AvroGenericSerializer<K> {
  public FastAvroGenericSerializer(Schema schema, FastSerdeCache cache) {
    super(new FastGenericDatumWriter<>(schema, cache));
  }
}
