package com.linkedin.venice.serializer;

import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.FastSpecificDatumReader;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;


/**
 * This class is leveraging linkedin-avro-fastserde-impl to speed up deserialization.
 * @param <T>
 */
public class FastAvroSpecificDeserializer<T extends SpecificRecord> extends AvroSpecificDeserializer<T> {
  public FastAvroSpecificDeserializer(Schema writer, Class<T> c, FastSerdeCache cache) {
    super(new FastSpecificDatumReader<>(writer, SpecificData.get().getSchema(c), cache));
  }
}
