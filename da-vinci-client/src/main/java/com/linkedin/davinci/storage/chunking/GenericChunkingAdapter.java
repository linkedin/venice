package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.serializer.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Read compute and write compute chunking adapter
 */
public class GenericChunkingAdapter<V extends GenericRecord> extends AbstractAvroChunkingAdapter<V> {
  public static final GenericChunkingAdapter INSTANCE = new GenericChunkingAdapter();

  /** Singleton */
  protected GenericChunkingAdapter() {}

  @Override
  protected RecordDeserializer<V> getDeserializer(String storeName, int writerSchemaId, int readerSchemaId, ReadOnlySchemaRepository schemaRepo, boolean fastAvroEnabled) {
    Schema writerSchema = schemaRepo.getValueSchema(storeName, writerSchemaId).getSchema();
    Schema readerSchema = schemaRepo.getValueSchema(storeName, readerSchemaId).getSchema();

    // TODO: Remove support for slow-avro
    if (fastAvroEnabled) {
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema);
    } else {
      return ComputableSerializerDeserializerFactory.getComputableAvroGenericDeserializer(writerSchema, readerSchema);
    }
  }
}
