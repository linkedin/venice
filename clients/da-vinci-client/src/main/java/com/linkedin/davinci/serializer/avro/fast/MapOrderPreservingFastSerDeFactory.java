package com.linkedin.davinci.serializer.avro.fast;

import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * Factory to create fast serializer/deserializer of supporting map ordering.
 */
public class MapOrderPreservingFastSerDeFactory {
  private static final Map<Schema, MapOrderPreservingFastSerializer> SERIALIZER_MAP = new VeniceConcurrentHashMap<>();
  private static final Map<SerializerDeserializerFactory.SchemaPairAndClassContainer, MapOrderPreservingFastDeserializer> DESERIALIZER_MAP =
      new VeniceConcurrentHashMap<>();

  public static <K> MapOrderPreservingFastSerializer<K> getSerializer(Schema schema) {
    return (MapOrderPreservingFastSerializer<K>) SERIALIZER_MAP
        .computeIfAbsent(schema, s -> new MapOrderPreservingFastSerializer<>(s));
  }

  public static MapOrderPreservingFastDeserializer getDeserializer(Schema writerSchema, Schema readerSchema) {
    return DESERIALIZER_MAP.computeIfAbsent(
        new SerializerDeserializerFactory.SchemaPairAndClassContainer(writerSchema, readerSchema, Object.class),
        o -> new MapOrderPreservingFastDeserializer(writerSchema, readerSchema));
  }
}
