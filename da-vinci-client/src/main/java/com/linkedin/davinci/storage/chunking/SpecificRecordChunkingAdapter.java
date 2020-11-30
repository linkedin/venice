package com.linkedin.davinci.storage.chunking;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.serializer.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class SpecificRecordChunkingAdapter<V extends SpecificRecord> extends AbstractAvroChunkingAdapter<V> {
  private final Class<V> valueClass;

  public SpecificRecordChunkingAdapter(Class<V> c) {
    this.valueClass = c;
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
}
