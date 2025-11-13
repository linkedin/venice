package com.linkedin.venice.serialization;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import java.util.function.BiFunction;
import java.util.function.Function;
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

  public AvroStoreDeserializerCache(SchemaReader schemaReader) {
    this(schemaReader::getValueSchema, FastSerializerDeserializerFactory::getFastAvroGenericDeserializer);
  }

  /**
   * Constructor that allows custom deserializer factory to be provided.
   * This enables support for custom serialization formats (e.g., Protocol Buffers)
   * while reusing the caching infrastructure.
   *
   * @param schemaGetter Function to retrieve schema by schema ID
   * @param deserializerGetter Function to create deserializer given writer and reader schemas
   */
  public AvroStoreDeserializerCache(
      IntFunction<Schema> schemaGetter,
      BiFunction<Schema, Schema, RecordDeserializer<T>> deserializerGetter) {
    this.cache = new BiIntKeyCache<>(
        (writerId, readerId) -> deserializerGetter.apply(schemaGetter.apply(writerId), schemaGetter.apply(readerId)));
  }

  /**
   * Constructor that allows custom deserializer factory to be provided.
   * This enables support for custom serialization formats (e.g., Protocol Buffers).
   * This variant assumes that the deserializer only needs the writer schema which is true for the case Protocol Buffers.
   *
   * @param schemaGetter Function to retrieve schema by schema ID
   * @param deserializerGetter Function to create deserializer given the writer schema
   */
  public AvroStoreDeserializerCache(
      IntFunction<Schema> schemaGetter,
      Function<Schema, RecordDeserializer<T>> deserializerGetter) {
    this.cache = new BiIntKeyCache<>((writerId, readerId) -> deserializerGetter.apply(schemaGetter.apply(writerId)));
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
