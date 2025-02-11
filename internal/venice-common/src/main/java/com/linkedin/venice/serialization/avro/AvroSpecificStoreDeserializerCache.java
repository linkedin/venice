package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.SparseConcurrentList;
import java.util.function.IntFunction;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


public class AvroSpecificStoreDeserializerCache<V extends SpecificRecord> implements StoreDeserializerCache<V> {
  private final IntFunction<Schema> schemaGetter;
  private final SparseConcurrentList<RecordDeserializer<V>> cache;
  private final Class<V> valueClass;

  public AvroSpecificStoreDeserializerCache(SchemaReader schemaReader, Class<V> valueClass) {
    this(schemaReader::getValueSchema, valueClass);
  }

  public AvroSpecificStoreDeserializerCache(
      ReadOnlySchemaRepository schemaRepository,
      String storeName,
      Class<V> valueClass) {
    this(id -> schemaRepository.getValueSchema(storeName, id).getSchema(), valueClass);
  }

  public AvroSpecificStoreDeserializerCache(Schema valueSchema, Class<V> valueClass) {
    this(id -> valueSchema, valueClass);
  }

  private AvroSpecificStoreDeserializerCache(IntFunction<Schema> schemaGetter, Class<V> valueClass) {
    this.schemaGetter = schemaGetter;
    this.cache = new SparseConcurrentList<>();
    this.valueClass = valueClass;
  }

  @Override
  public RecordDeserializer<V> getDeserializer(int writerSchemaId, int readerSchemaId) {
    // TODO we can also throw Unsupported exception here instead of ignoring the readerSchemaId parameter.
    return cache.computeIfAbsent(
        writerSchemaId,
        (schemaId) -> FastSerializerDeserializerFactory
            .getFastAvroSpecificDeserializer(schemaGetter.apply(schemaId), valueClass));
  }

  @Override
  public RecordDeserializer<V> getDeserializer(int writerSchemaId) {
    return cache.computeIfAbsent(
        writerSchemaId,
        (schemaId) -> FastSerializerDeserializerFactory
            .getFastAvroSpecificDeserializer(schemaGetter.apply(schemaId), valueClass));
  }
}
