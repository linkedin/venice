package com.linkedin.venice.serializer.avro;

import com.linkedin.venice.serializer.AvroGenericDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * {@code MapOrderPreservingDeserializer} is a Avro deserializer using {@link MapOrderPreservingDatumReader}.
 */
public class MapOrderPreservingDeserializer extends AvroGenericDeserializer<GenericRecord> {
  /**
   * Constructor is made package-private so that users of this class should create it via {@link MapOrderingPreservingSerDeFactory}
   */
  MapOrderPreservingDeserializer(Schema writer, Schema reader) {
    super(new MapOrderPreservingDatumReader<>(writer, reader));
  }
}
