package com.linkedin.venice.serializer.avro;

import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * This class is a factory that creates {@link MapOrderPreservingSerializer} and {@link MapOrderPreservingDeserializer}
 * with given schemas and cache them.
 */
public class MapOrderingPreservingSerDeFactory extends SerializerDeserializerFactory {
  private static final Map<Schema, MapOrderPreservingSerializer<?>> SERIALIZER_MAP = new VeniceConcurrentHashMap<>();
  private static final Map<SchemaPairAndClassContainer, MapOrderPreservingDeserializer> DESERIALIZER_MAP =
      new VeniceConcurrentHashMap<>();

  public static <K> MapOrderPreservingSerializer<K> getSerializer(Schema schema) {
    return (MapOrderPreservingSerializer<K>) SERIALIZER_MAP
        .computeIfAbsent(schema, s -> new MapOrderPreservingSerializer<>(s));
  }

  public static MapOrderPreservingDeserializer getDeserializer(Schema writerSchema, Schema readerSchema) {
    return DESERIALIZER_MAP.computeIfAbsent(
        new SchemaPairAndClassContainer(writerSchema, readerSchema, Object.class),
        o -> new MapOrderPreservingDeserializer(writerSchema, readerSchema));
  }
}
