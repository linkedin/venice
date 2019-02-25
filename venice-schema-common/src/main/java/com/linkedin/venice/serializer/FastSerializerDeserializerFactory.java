package com.linkedin.venice.serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class FastSerializerDeserializerFactory extends SerializerDeserializerFactory {
  private static Map<SchemaPairAndClassContainer, AvroGenericDeserializer<Object>>
      avroFastGenericDeserializerMap = new ConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, AvroSpecificDeserializer<? extends SpecificRecord>>
      avroFastSpecificDeserializerMap = new ConcurrentHashMap<>();

  public static <V> RecordDeserializer<V> getFastAvroGenericDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, Object.class);
    return (AvroGenericDeserializer<V>) avroFastGenericDeserializerMap.computeIfAbsent(
        container, key -> new FastAvroGenericDeserializer(key.writer, key.reader));
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getFastAvroSpecificDeserializer(Schema writer, Class<V> c) {
    return getFastAvroSpecificDeserializer(writer, c, null);
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getFastAvroSpecificDeserializer(Schema writer, Class<V> c,
      AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl) {
    return getAvroSpecificDeserializerInternal(writer, c,
        container -> avroFastSpecificDeserializerMap.computeIfAbsent(container,
            k -> new FastAvroSpecificDeserializer<V>(container.writer, container.c, multiGetEnvelopeIterableImpl)));
  }
}