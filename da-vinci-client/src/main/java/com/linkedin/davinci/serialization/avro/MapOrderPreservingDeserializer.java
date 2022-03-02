package com.linkedin.davinci.serialization.avro;

import com.linkedin.venice.serializer.AvroGenericDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class MapOrderPreservingDeserializer extends AvroGenericDeserializer<GenericRecord> {

  /**
   * Constructor is made package-private so that users of this class should create it via {@link MapOrderingPreservingSerDeFactory}
   */
  MapOrderPreservingDeserializer(Schema writer, Schema reader) {
    super(new MapOrderPreservingDatumReader<>(writer, reader));
  }
}
