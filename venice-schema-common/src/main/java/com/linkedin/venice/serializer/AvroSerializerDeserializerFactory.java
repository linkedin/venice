package com.linkedin.venice.serializer;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * For one given store, the client only needs the followings:
 * 1. One {@link AvroGenericSerializer} for key serialization;
 * 2. Multiple {@link AvroGenericDeserializer} / {@link AvroSpecificDeserializer} for value deserialization;
 * By using this implementation, we can reuse the serializer/deserializer to improve the
 * performance of key serialization/value deserialization.
 */
public class AvroSerializerDeserializerFactory {
  // Class works as the key of caching map for AvroSerializerDeserializerFactory
  private static class SchemaPairAndClassContainer {
    public Schema writer;
    public Schema reader;
    public Class c;

    public SchemaPairAndClassContainer(Schema writer, Schema reader, Class c) {
      this.writer = writer;
      this.reader = reader;
      this.c = c;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SchemaPairAndClassContainer that = (SchemaPairAndClassContainer) o;

      if (!writer.equals(that.writer)) return false;
      if (!reader.equals(that.reader)) return false;
      return c.equals(that.c);

    }

    @Override
    public int hashCode() {
      int result = writer.hashCode();
      result = 31 * result + reader.hashCode();
      result = 31 * result + c.hashCode();
      return result;
    }
  }

  private static Map<Schema, AvroGenericSerializer> genericSerializerMap = new ConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, AvroGenericDeserializer<Object>> genericDeserializerMap =
      new ConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, AvroSpecificDeserializer<? extends SpecificRecord>>
      specificDeserializerMap = new ConcurrentHashMap<>();

  public static <K> RecordSerializer<K> getAvroGenericSerializer(Schema schema) {
    genericSerializerMap.computeIfAbsent(schema,
        key -> {
          return new AvroGenericSerializer(key);
        });
    return genericSerializerMap.get(schema);
  }

  public static <V> RecordDeserializer<V> getAvroGenericDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, Object.class);
    genericDeserializerMap.computeIfAbsent(container, key -> {
      return new AvroGenericDeserializer<Object>(key.writer, key.reader);
    });
    return (AvroGenericDeserializer<V>)genericDeserializerMap.get(container);
  }

  /**
   * This function is assuming that both writer and reader are using the same schema.
   * @param schema
   * @param <V>
   * @return
   */
  public static <V> RecordDeserializer<V> getAvroGenericDeserializer(Schema schema) {
    return getAvroGenericDeserializer(schema, schema);
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializer(Schema writer, Class<V> c) {
    Schema reader = SpecificData.get().getSchema(c);
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, c);
    specificDeserializerMap.computeIfAbsent(container, key -> {
      return new AvroSpecificDeserializer<V>(key.writer, key.c);
    });

    return (AvroSpecificDeserializer<V>)specificDeserializerMap.get(container);
  }

  /**
   * This function is assuming that both writer and reader are using the same schema defined in {@param c}.
   * @param c
   * @param <V>
   * @return
   */
  public static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializer(Class<V> c) {
    Schema schema = SpecificData.get().getSchema(c);
    return getAvroSpecificDeserializer(schema, c);
  }
}
