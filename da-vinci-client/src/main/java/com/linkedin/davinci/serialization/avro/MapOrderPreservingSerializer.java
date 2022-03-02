package com.linkedin.davinci.serialization.avro;

import com.linkedin.venice.serializer.AvroSerializer;
import org.apache.avro.Schema;


public class MapOrderPreservingSerializer<K> extends AvroSerializer<K> {

  /**
   * Constructor is made package-private so that users of this class should create it via {@link MapOrderingPreservingSerDeFactory}
   * @param schema
   */
  MapOrderPreservingSerializer(Schema schema) {
    super(new MapOrderPreservingGenericDatumWriter<>(schema), new MapOrderPreservingSpecificDatumWriter<>(schema));
  }
}
