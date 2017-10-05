package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import org.apache.avro.Schema;


public class VsonGenericStoreClientImpl<K, V> extends AvroGenericStoreClientImpl<K, V> {
  public VsonGenericStoreClientImpl(TransportClient transportClient, String storeName) {
    this(transportClient, storeName, true);
  }

  private VsonGenericStoreClientImpl(TransportClient transportClient, String storeName, boolean needSchemaReader) {
    super(transportClient, storeName, needSchemaReader);
  }

  @Override
  protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
    return new VsonGenericStoreClientImpl<K, V> (getTransportClient().getCopyIfNotUsableInCallback(), getStoreName(), false);
  }

  @Override
  protected RecordDeserializer<V> getDeserializerFromFactory(Schema writer, Schema reader) {
    return SerializerDeserializerFactory.getVsonDeserializer(writer, reader);
  }

  @Override
  protected void init() {
    this.keySerializer =
        SerializerDeserializerFactory.getVsonSerializer(getSchemaReader().getKeySchema());
    this.multiGetRequestSerializer =
        SerializerDeserializerFactory.getVsonSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
  }

}
