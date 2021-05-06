package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.serializer.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class SpecificRecordChunkingAdapter<V extends SpecificRecord> extends AbstractAvroChunkingAdapter<V> {
  private final Class<V> valueClass;
  private final CompressorFactory compressorFactory;

  public SpecificRecordChunkingAdapter(Class<V> c, CompressorFactory compressorFactory) {
    this.valueClass = c;
    this.compressorFactory = compressorFactory;
  }

  @Override
  protected RecordDeserializer<V> getDeserializer(String storeName, int schemaId, ReadOnlySchemaRepository schemaRepo, boolean fastAvroEnabled) {
    Schema writerSchema = schemaRepo.getValueSchema(storeName, schemaId).getSchema();

    // TODO: Remove support for slow-avro
    if (fastAvroEnabled) {
      return FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(writerSchema, valueClass);
    } else {
      return ComputableSerializerDeserializerFactory.getComputableAvroSpecificDeserializer(writerSchema, valueClass);
    }
  }

  @Override
  protected CompressorFactory getCompressorFactory() {
    return compressorFactory;
  }
}
