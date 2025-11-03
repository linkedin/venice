package com.linkedin.venice.fastclient;

import com.linkedin.venice.serializer.RecordSerializer;
import org.apache.avro.Schema;


/**
 * Factory interface for creating key serializers.
 * This allows clients to provide custom serializers (e.g., Protocol Buffer serializers)
 * without needing to extend or modify the core fast client implementation.
 *
 * <p>The factory is called during client initialization when the key schema is available.
 * Implementations should be thread-safe as they may be called from multiple threads.
 *
 * <p>Example usage for Protocol Buffers:
 * <pre>{@code
 * SerializerFactory<MyKeyProto> protoKeyFactory = 
 *     (keySchema) -> new ProtoToAvroRecordSerializer<>(keySchema);
 *
 * ClientConfig config = new ClientConfig.ClientConfigBuilder()
 *     .setKeySerializerFactory(protoKeyFactory)
 *     .build();
 * }</pre>
 *
 * @param <K> the key type
 */
@FunctionalInterface
public interface SerializerFactory<K> {
  /**
   * Create a serializer for the given key schema.
   *
   * @param keySchema the Avro schema for keys (keys are always Avro format in Venice storage)
   * @return a record serializer that can serialize keys to Avro bytes; must not return null
   * @throws IllegalArgumentException if the schema is not compatible with the serializer
   */
  RecordSerializer<K> createSerializer(Schema keySchema);
}
