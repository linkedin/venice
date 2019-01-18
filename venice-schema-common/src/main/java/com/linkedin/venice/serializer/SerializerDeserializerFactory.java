package com.linkedin.venice.serializer;

import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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
public class SerializerDeserializerFactory {
  // Class works as the key of caching map for SerializerDeserializerFactory
  protected static class SchemaPairAndClassContainer {
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

      if (c == null) {
        return that.c == null;
      }

      return c.equals(that.c);
    }

    @Override
    public int hashCode() {
      int result = writer.hashCode();
      result = 31 * result + reader.hashCode();
      if (c != null) {
        result = 31 * result + c.hashCode();
      }
      return result;
    }
  }

  private static Map<Schema, AvroGenericSerializer> avroGenericSerializerMap = new ConcurrentHashMap<>();
  private static Map<Schema, VsonAvroGenericSerializer> vsonGenericSerializerMap = new ConcurrentHashMap<>();

  private static Map<SchemaPairAndClassContainer, AvroGenericDeserializer<Object>> avroGenericDeserializerMap =
      new ConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, AvroSpecificDeserializer<? extends SpecificRecord>>
      avroSpecificDeserializerMap = new ConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, VsonAvroGenericDeserializer> vsonGenericDeserializerMap =
      new ConcurrentHashMap<>();

  public static <K> RecordSerializer<K> getAvroGenericSerializer(Schema schema) {
    return avroGenericSerializerMap.computeIfAbsent(schema, key -> new AvroGenericSerializer(key));
  }

  public static <K> RecordSerializer<K> getVsonSerializer(Schema schema) {
     return vsonGenericSerializerMap.computeIfAbsent(schema, key -> new VsonAvroGenericSerializer<> (key));
  }

  public static <K> RecordDeserializer<K> getVsonDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, null);
    return vsonGenericDeserializerMap.computeIfAbsent(
        container, key -> new VsonAvroGenericDeserializer<> (key.writer, key.reader));
  }

  public static <V> RecordDeserializer<V> getAvroGenericDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, Object.class);
    return (AvroGenericDeserializer<V>) avroGenericDeserializerMap.computeIfAbsent(
        container, key -> new AvroGenericDeserializer<>(key.writer, key.reader));
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
    return getAvroSpecificDeserializer(writer, c, null);
  }

  /**
   * This function is assuming that both writer and reader are using the same schema defined in {@param c}.
   * @param c
   * @param <V>
   * @return
   */
  public static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializer(Class<V> c, AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl) {
    Schema writer = SpecificData.get().getSchema(c);
    return getAvroSpecificDeserializer(writer, c, multiGetEnvelopeIterableImpl);
  }

  /**
   * This function is assuming that both writer and reader are using the same schema defined in {@param c}.
   * @param c
   * @param <V>
   * @return
   */
  public static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializer(Class<V> c) {
    return getAvroSpecificDeserializer(c, null);
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializer(Schema writer, Class<V> c, AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl) {
    return getAvroSpecificDeserializerInternal(writer, c,
        container -> avroSpecificDeserializerMap.computeIfAbsent(container,
            k -> new AvroSpecificDeserializer<V>(container.writer, container.c, multiGetEnvelopeIterableImpl)));
  }

  protected static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializerInternal(
      Schema writer,
      Class<V> c,
      Function<SchemaPairAndClassContainer, AvroSpecificDeserializer<? extends SpecificRecord>> deserializerGenerator) {
    Schema reader = SpecificData.get().getSchema(c);
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, c);
    return (AvroSpecificDeserializer<V>) deserializerGenerator.apply(container);
  }
}