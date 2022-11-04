package com.linkedin.venice.fastclient;

import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import org.apache.avro.Schema;


/**
 * This class is used to support a legacy format from Voldemort.
 */
@Deprecated
public class DispatchingVsonStoreClient<K, V> extends DispatchingAvroGenericStoreClient<K, V> {
  public DispatchingVsonStoreClient(StoreMetadata metadata, ClientConfig config) {
    super(metadata, config);
  }

  @Override
  protected RecordSerializer getKeySerializer(Schema keySchema) {
    return SerializerDeserializerFactory.getVsonSerializer(keySchema);
  }

  @Override
  protected RecordDeserializer<V> getValueDeserializer(Schema writerSchema, Schema readerSchema) {
    return SerializerDeserializerFactory.getVsonDeserializer(writerSchema, readerSchema);
  }
}
