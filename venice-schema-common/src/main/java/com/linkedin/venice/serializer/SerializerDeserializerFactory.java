package com.linkedin.venice.serializer;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;


/**
 * For one given store, the client only needs the followings:
 * 1. One {@link AvroSerializer} for key serialization;
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
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      SchemaPairAndClassContainer that = (SchemaPairAndClassContainer) o;

      if (!writer.equals(that.writer))
        return false;
      if (!reader.equals(that.reader))
        return false;

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

  private static Map<Schema, AvroSerializer> avroGenericSerializerMap = new VeniceConcurrentHashMap<>();
  private static Map<Schema, VsonAvroGenericSerializer> vsonGenericSerializerMap = new VeniceConcurrentHashMap<>();

  private static Map<SchemaPairAndClassContainer, AvroGenericDeserializer<Object>> avroGenericDeserializerMap =
      new VeniceConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, AvroSpecificDeserializer<? extends SpecificRecord>> avroSpecificDeserializerMap =
      new VeniceConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, VsonAvroGenericDeserializer> vsonGenericDeserializerMap =
      new VeniceConcurrentHashMap<>();

  public static <K> RecordSerializer<K> getAvroGenericSerializer(Schema schema) {
    return avroGenericSerializerMap.computeIfAbsent(schema, key -> new AvroSerializer(key));
  }

  public static <K> RecordSerializer<K> getAvroGenericSerializer(Schema schema, boolean buffered) {
    return avroGenericSerializerMap.computeIfAbsent(schema, key -> new AvroSerializer(key, buffered));
  }

  public static <K> RecordSerializer<K> getVsonSerializer(Schema schema) {
    return vsonGenericSerializerMap.computeIfAbsent(schema, key -> new VsonAvroGenericSerializer<>(key));
  }

  public static <K> RecordDeserializer<K> getVsonDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, null);
    return vsonGenericDeserializerMap
        .computeIfAbsent(container, key -> new VsonAvroGenericDeserializer<>(key.writer, key.reader));
  }

  public static <V> RecordDeserializer<V> getAvroGenericDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, Object.class);
    return (AvroGenericDeserializer<V>) avroGenericDeserializerMap
        .computeIfAbsent(container, key -> new AvroGenericDeserializer<>(key.writer, key.reader));
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

  /**
   * This function is assuming that both writer and reader are using the same schema defined in {@param c}.
   * @param c
   * @param <V>
   * @return
   */
  public static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializer(Class<V> c) {
    Schema writer = SpecificData.get().getSchema(c);
    return getAvroSpecificDeserializer(writer, c);
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getAvroSpecificDeserializer(
      Schema writer,
      Class<V> c) {
    return getAvroSpecificDeserializerInternal(
        writer,
        c,
        container -> avroSpecificDeserializerMap
            .computeIfAbsent(container, k -> new AvroSpecificDeserializer<V>(container.writer, container.c)));
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
