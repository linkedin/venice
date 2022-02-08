package com.linkedin.davinci.serialization.avro;

import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import org.apache.avro.Schema;
import java.util.Map;


/**
 * This class is a factory that creates {@link MapOrderPreservingSerializer} and {@link MapOrderPreservingDeserializer}
 * with given schemas and cache them.
 */
public class MapOrderingPreservingSeDeFactory extends SerializerDeserializerFactory {

  private static final Map<Schema, MapOrderPreservingSerializer<?>> serializerMap
      = new VeniceConcurrentHashMap<>();
  private static final Map<SchemaPairAndClassContainer, MapOrderPreservingDeserializer> deserializerMap
      = new VeniceConcurrentHashMap<>();

  public static <K> MapOrderPreservingSerializer<K> getSerializer(Schema schema) {
    return (MapOrderPreservingSerializer<K>) serializerMap.computeIfAbsent(schema, s -> new MapOrderPreservingSerializer<>(s));
  }

  public static MapOrderPreservingDeserializer getDeserializer(Schema writerSchema, Schema readerSchema) {
    return deserializerMap.computeIfAbsent(new SchemaPairAndClassContainer(writerSchema, readerSchema, Object.class),
        o -> new MapOrderPreservingDeserializer(writerSchema, readerSchema));
  }
}
