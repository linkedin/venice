package com.linkedin.venice.serialization;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.BiIntFunction;
import com.linkedin.venice.utils.SparseConcurrentList;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import org.apache.avro.Schema;


/**
 * Container for the deserializers of a single store.
 */
public class AvroStoreDeserializerCache<T> implements StoreDeserializerCache<T> {
  SparseConcurrentList<SparseConcurrentList<RecordDeserializer<T>>> genericDeserializers = new SparseConcurrentList<>();
  BiIntFunction<RecordDeserializer<T>> deserializerGenerator;

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

  public AvroStoreDeserializerCache(
      IntFunction<Schema> schemaGetter,
      BiFunction<Schema, Schema, RecordDeserializer<T>> deserializerGetter) {
    this((writerId, readerId) -> deserializerGetter.apply(schemaGetter.apply(writerId), schemaGetter.apply(readerId)));
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

  public AvroStoreDeserializerCache(BiIntFunction<RecordDeserializer<T>> deserializerGenerator) {
    this.deserializerGenerator = deserializerGenerator;
  }

  public RecordDeserializer<T> getDeserializer(int writerSchemaId, int readerSchemaId) {
    SparseConcurrentList<RecordDeserializer<T>> innerList =
        genericDeserializers.computeIfAbsent(writerSchemaId, SparseConcurrentList.SUPPLIER);
    RecordDeserializer<T> deserializer = innerList.get(readerSchemaId);
    if (deserializer == null) {
      deserializer = deserializerGenerator.apply(writerSchemaId, readerSchemaId);
      innerList.set(readerSchemaId, deserializer);
    }
    return deserializer;
  }
}
