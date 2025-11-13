package com.linkedin.venice.fastclient;

import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.Schema;


/**
 * Factory interface for creating value deserializers.
 * This allows clients to provide custom deserializers (e.g., Protocol Buffer deserializers)
 * without needing to extend or modify the core fast client implementation.
 *
 * <p>The factory is called when a new value schema is encountered. The deserializer will be
 * cached and reused for subsequent reads with the same schema pair.
 *
 * <p>Implementations should be thread-safe as they may be called from multiple threads.
 *
 * <p>Example usage for Protocol Buffers:
 * <pre>{@code
 * DeserializerFactory<MyValueProto> protoValueFactory =
 *     (writerSchema) -> new RecordDeserializerToProto<>(writerSchema, MyValueProto.class);
 *
 * ClientConfig config = new ClientConfig.ClientConfigBuilder()
 *     .setValueDeserializerFactory(protoValueFactory)
 *     .build();
 * }</pre>
 *
 * @param <V> the value type
 */
@FunctionalInterface
public interface DeserializerFactory<V> {
  /**
   * Create a deserializer for the given writer and reader schemas.
   *
   * <p>Venice supports schema evolution, so the writer schema (used when data was written)
   * may differ from the reader schema (used when reading). The deserializer should handle
   * this schema evolution appropriately.
   *
   * @param writerSchema the schema that was used when the value was written
   * @return a record deserializer that can deserialize Avro bytes to the value type; must not return null
   */
  RecordDeserializer<V> createDeserializer(Schema writerSchema);
}
