package com.linkedin.venice.serializer.avro;

import com.linkedin.venice.serializer.AvroSerializer;
import org.apache.avro.Schema;


/**
 * {@code MapOrderPreservingSerializer} is {@code AvroSerializer} that maintains a consistent ordering of map entries by
 * using {@link MapOrderPreservingGenericDatumWriter} and {@link MapOrderPreservingSpecificDatumWriter}.
 */
public class MapOrderPreservingSerializer<K> extends AvroSerializer<K> {
  /**
   * Constructor is made package-private so that users of this class should create it via {@link MapOrderingPreservingSerDeFactory}
   * @param schema
   */
  MapOrderPreservingSerializer(Schema schema) {
    super(new MapOrderPreservingGenericDatumWriter<>(schema), new MapOrderPreservingSpecificDatumWriter<>(schema));
  }
}
