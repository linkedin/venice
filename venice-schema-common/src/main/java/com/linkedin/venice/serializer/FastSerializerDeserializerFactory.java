package com.linkedin.venice.serializer;

import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

public class FastSerializerDeserializerFactory extends SerializerDeserializerFactory {
  private static FastSerdeCache cache = FastSerdeCache.getDefaultInstance();

  private static Map<SchemaPairAndClassContainer, AvroGenericDeserializer<Object>>
      avroFastGenericDeserializerMap = new ConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, AvroSpecificDeserializer<? extends SpecificRecord>>
      avroFastSpecificDeserializerMap = new ConcurrentHashMap<>();

  // Verify whether fast-avro could generate a fast specific deserializer, but there is no guarantee that
  // the success of all other fast specific deserializer generation in the future.
  public static void verifyWhetherFastSpecificSerializerWorks(Class<? extends SpecificRecord> specificClass) {
    for (Class<? extends SpecificRecord> c : Arrays.asList(specificClass, MultiGetResponseRecordV1.class)){
      Schema schema = SpecificData.get().getSchema(c);
      try {
        cache.buildFastSpecificDeserializer(schema, schema);
      } catch (Exception e) {
        throw new VeniceException("Failed to generate fast specific de-serializer for class: " + c, e);
      }
    }
  }

  // Verify whether fast-avro could generate a fast generic deserializer, but there is no guarantee that
  // the success of all other fast generic deserializer generation in the future.
  public static void verifyWhetherFastGenericSerializerWorks() {
    // Leverage the following schema as the input for testing
    Schema schema = MultiGetResponseRecordV1.SCHEMA$;
    try {
      cache.buildFastGenericDeserializer(schema, schema);
    } catch (Exception e) {
      throw new VeniceException("Failed to generate fast generic de-serializer for Avro schema: " + schema, e);
    }
  }

  public static <V> RecordDeserializer<V> getFastAvroGenericDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, Object.class);
    return (AvroGenericDeserializer<V>) avroFastGenericDeserializerMap.computeIfAbsent(
        container, key -> new FastAvroGenericDeserializer(key.writer, key.reader, cache));
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getFastAvroSpecificDeserializer(Schema writer, Class<V> c) {
    return getFastAvroSpecificDeserializer(writer, c, null);
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getFastAvroSpecificDeserializer(Schema writer, Class<V> c,
      AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl) {
    return getAvroSpecificDeserializerInternal(writer, c,
        container -> avroFastSpecificDeserializerMap.computeIfAbsent(container,
            k -> new FastAvroSpecificDeserializer<V>(container.writer, container.c, cache, multiGetEnvelopeIterableImpl)));
  }
}