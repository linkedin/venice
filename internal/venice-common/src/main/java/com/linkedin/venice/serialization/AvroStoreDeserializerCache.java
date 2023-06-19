package com.linkedin.venice.serialization;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
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
public class AvroStoreDeserializerCache<T> implements StoreDeserializerCache<T> {
  private final BiIntKeyCache<RecordDeserializer<T>> cache;

  public AvroStoreDeserializerCache(
      ReadOnlySchemaRepository schemaRepository,
      String storeName,
      boolean fastAvroEnabled) {
    this(
        id -> schemaRepository.getValueSchema(storeName, id).getSchema(),
        fastAvroEnabled
            ? FastSerializerDeserializerFactory::getFastAvroGenericDeserializer
            : SerializerDeserializerFactory::getAvroGenericDeserializer);
  }

  private AvroStoreDeserializerCache(
      IntFunction<Schema> schemaGetter,
      BiFunction<Schema, Schema, RecordDeserializer<T>> deserializerGetter) {
    this.cache = new BiIntKeyCache<>(
        (writerId, readerId) -> deserializerGetter.apply(schemaGetter.apply(writerId), schemaGetter.apply(readerId)));
  }

  public AvroStoreDeserializerCache(
      ReadOnlySchemaRepository schemaRepository,
      String storeName,
      Class specificRecordClass) {
    this(
        id -> schemaRepository.getValueSchema(storeName, id).getSchema(),
        (writerSchema, readerSchema) -> FastSerializerDeserializerFactory
            .getFastAvroSpecificDeserializer(writerSchema, specificRecordClass));
  }

  public RecordDeserializer<T> getDeserializer(int writerSchemaId, int readerSchemaId) {
    return this.cache.get(writerSchemaId, readerSchemaId);
  }
}
