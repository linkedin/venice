package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.serializer.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.Schema;


/**
 * Read compute and write compute chunking adapter
 */
public class GenericChunkingAdapter<V> extends AbstractAvroChunkingAdapter<V> {
  public static final GenericChunkingAdapter INSTANCE = new GenericChunkingAdapter();

  /** Singleton */
  protected GenericChunkingAdapter() {}

  @Override
  protected RecordDeserializer<V> getDeserializer(String storeName, int schemaId, ReadOnlySchemaRepository schemaRepo, boolean fastAvroEnabled) {
    Schema writerSchema = schemaRepo.getValueSchema(storeName, schemaId).getSchema();
    Schema latestValueSchema = schemaRepo.getLatestValueSchema(storeName).getSchema();

    // TODO: Remove support for slow-avro
    if (fastAvroEnabled) {
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, latestValueSchema);
    } else {
      return ComputableSerializerDeserializerFactory.getComputableAvroGenericDeserializer(writerSchema, latestValueSchema);
    }
  }
}
