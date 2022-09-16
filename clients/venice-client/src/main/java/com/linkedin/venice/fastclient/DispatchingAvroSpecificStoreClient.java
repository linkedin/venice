package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


public class DispatchingAvroSpecificStoreClient<K, V extends SpecificRecord>
    extends DispatchingAvroGenericStoreClient<K, V> implements AvroSpecificStoreClient<K, V> {
  private final Class<V> valueClass;

  public DispatchingAvroSpecificStoreClient(StoreMetadata metadata, ClientConfig config) {
    super(metadata, config);
    if (config.getSpecificValueClass() == null) {
      throw new VeniceClientException(
          "SpecificValueClass in ClientConfig shouldn't be null when constructing a specific store client.");
    }
    this.valueClass = config.getSpecificValueClass();

    FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(this.valueClass);
  }

  @Override
  protected RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
    Schema writerSchema = getStoreMetadata().getValueSchema(schemaId);
    if (writerSchema == null) {
      throw new VeniceClientException(
          "Failed to get value schema for store: " + getStoreName() + " and id: " + schemaId);
    }
    return FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(writerSchema, valueClass);
  }
}
