package com.linkedin.venice.serializer;

import com.linkedin.avro.fastserde.FastGenericDatumWriter;
import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.FastSpecificDatumWriter;
import org.apache.avro.Schema;


/**
 * This class is leveraging linkedin-avro-fastserde-impl to speed up serialization,
 * and so far it doesn't work for Avro-1.4, and it will fail back to regular Avro.
 * @param <K>
 */
public class FastAvroSerializer<K> extends AvroSerializer<K> {
  public FastAvroSerializer(Schema schema, FastSerdeCache cache) {
    super(new FastGenericDatumWriter<>(schema, cache), new FastSpecificDatumWriter<>(schema, cache));
  }

  public FastAvroSerializer(Schema schema, FastSerdeCache cache, boolean buffered) {
    super(new FastGenericDatumWriter<>(schema, cache), new FastSpecificDatumWriter<>(schema, cache), buffered);
  }
}
