package com.linkedin.davinci.consumer;

import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import org.apache.avro.Schema;


/**
 * Container for the deserializers of a single store.
 */
public class RmdDeserializerCache<T> implements StoreDeserializerCache<T> {
  private final BiIntKeyCache<RecordDeserializer<T>> cache;

  public RmdDeserializerCache(
      ReplicationMetadataSchemaRepository rmdSchemaRepo,
      String storeName,
      int protocolId,
      boolean fastAvroEnabled) {
    this(
        (valueSchemaId) -> rmdSchemaRepo.getReplicationMetadataSchemaById(storeName, valueSchemaId).getSchema(),
        fastAvroEnabled
            ? FastSerializerDeserializerFactory::getFastAvroGenericDeserializer
            : SerializerDeserializerFactory::getAvroGenericDeserializer);
  }

  private RmdDeserializerCache(
      IntFunction<Schema> schemaGetter,
      BiFunction<Schema, Schema, RecordDeserializer<T>> deserializerGetter) {
    this.cache = new BiIntKeyCache<>(
        (writerId, readerId) -> deserializerGetter.apply(schemaGetter.apply(writerId), schemaGetter.apply(readerId)));
  }

  public RecordDeserializer<T> getDeserializer(int writerSchemaId, int readerSchemaId) {
    return this.cache.get(writerSchemaId, readerSchemaId);
  }

  @Override
  public RecordDeserializer<T> getDeserializer(int writerSchemaId) {
    throw new UnsupportedOperationException(
        "getDeserializer by only writeSchemaId is not supported by " + this.getClass().getSimpleName());
  }
}
