package com.linkedin.venice.schema.avro;

import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSpecificDeserializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


public class ComputableSerializerDeserializerFactory extends SerializerDeserializerFactory {
  private static Map<SchemaPairAndClassContainer, AvroGenericDeserializer<Object>>
      avroComputableGenericDeserializerMap = new VeniceConcurrentHashMap<>();
  private static Map<SchemaPairAndClassContainer, AvroSpecificDeserializer<? extends SpecificRecord>>
      avroComputableSpecificDeserializerMap = new VeniceConcurrentHashMap<>();

  public static <V> RecordDeserializer<V> getComputableAvroGenericDeserializer(Schema writer, Schema reader) {
    SchemaPairAndClassContainer container = new SchemaPairAndClassContainer(writer, reader, Object.class);
    return (AvroGenericDeserializer<V>) avroComputableGenericDeserializerMap.computeIfAbsent(
        container, key -> new ComputableAvroGenericDeserializer(key.writer, key.reader));
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getComputableAvroSpecificDeserializer(Schema writer, Class<V> c) {
    return getComputableAvroSpecificDeserializer(writer, c, null);
  }

  public static <V extends SpecificRecord> RecordDeserializer<V> getComputableAvroSpecificDeserializer(Schema writer, Class<V> c, AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl) {
    return getAvroSpecificDeserializerInternal(writer, c,
        container -> avroComputableSpecificDeserializerMap.computeIfAbsent(container,
            k -> new ComputableAvroSpecificDeserializer<V>(container.writer, container.c, multiGetEnvelopeIterableImpl)));
  }
}
