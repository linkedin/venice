package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
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
  protected RecordDeserializer<V> getDataRecordDeserializer(int schemaId) {
    Schema readerSchema = metadata.getLatestValueSchema();
    if (readerSchema == null) {
      throw new VeniceClientException("Failed to get latest value schema for store: " + metadata.getStoreName());
    }
    Schema writerSchema = metadata.getValueSchema(schemaId);
    if (writerSchema == null) {
      throw new VeniceClientException(
          "Failed to get writer schema with id: " + schemaId + " from store: " + metadata.getStoreName());
    }
    return SerializerDeserializerFactory.getVsonDeserializer(writerSchema, readerSchema);
  }
}
