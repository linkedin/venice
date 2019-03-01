package com.linkedin.venice.serializer;

import com.linkedin.avro.fastserde.FastGenericDatumReader;
import com.linkedin.avro.fastserde.FastSerdeCache;
import org.apache.avro.Schema;


/**
 *  * This class is leveraging linkedin-avro-fastserde-impl to speed up deserialization.
 * @param <V>
 */
public class FastAvroGenericDeserializer<V> extends AvroGenericDeserializer<V> {
  public FastAvroGenericDeserializer(Schema writer, Schema reader, FastSerdeCache cache) {
    super(new FastGenericDatumReader<>(writer, reader, cache));
  }
}
