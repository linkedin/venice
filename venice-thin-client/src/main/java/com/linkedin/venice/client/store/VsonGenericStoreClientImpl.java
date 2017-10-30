package com.linkedin.venice.client.store;

import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;


public class VsonGenericStoreClientImpl<K, V> extends AvroGenericStoreClientImpl<K, V> {
  public VsonGenericStoreClientImpl(TransportClient transportClient, String storeName) {
    this(transportClient, storeName, true, AbstractAvroStoreClient.getDefaultDeserializationExecutor());
  }

  public VsonGenericStoreClientImpl(TransportClient transportClient, String storeName, Executor deserializationExecutor) {
    this(transportClient, storeName, true, deserializationExecutor);
  }

  private VsonGenericStoreClientImpl(TransportClient transportClient, String storeName, boolean needSchemaReader, Executor deserializationExecutor) {
    super(transportClient, storeName, needSchemaReader, deserializationExecutor);
  }

  @Override
  protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
    return new VsonGenericStoreClientImpl<K, V> (getTransportClient().getCopyIfNotUsableInCallback(),
        getStoreName(), false, getDeserializationExecutor());
  }

  @Override
  protected RecordDeserializer<V> getDeserializerFromFactory(Schema writer, Schema reader) {
    return SerializerDeserializerFactory.getVsonDeserializer(writer, reader);
  }

  @Override
  protected void initSerializer() {
    this.keySerializer =
        SerializerDeserializerFactory.getVsonSerializer(getSchemaReader().getKeySchema());
    this.multiGetRequestSerializer =
        SerializerDeserializerFactory.getVsonSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
  }

}
