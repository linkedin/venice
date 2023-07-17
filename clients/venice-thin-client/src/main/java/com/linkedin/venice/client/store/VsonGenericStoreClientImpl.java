package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import org.apache.avro.Schema;


@Deprecated
public class VsonGenericStoreClientImpl<K, V> extends AvroGenericStoreClientImpl<K, V> {
  public VsonGenericStoreClientImpl(TransportClient transportClient, ClientConfig clientConfig) {
    this(transportClient, true, clientConfig);
  }

  private VsonGenericStoreClientImpl(
      TransportClient transportClient,
      boolean needSchemaReader,
      ClientConfig clientConfig) {
    super(transportClient, needSchemaReader, clientConfig);
  }

  @Override
  protected RecordDeserializer<V> getDeserializerFromFactory(Schema writer, Schema reader) {
    return SerializerDeserializerFactory.getVsonDeserializer(writer, reader);
  }

  @Override
  protected void initSerializer() {
    if (needSchemaReader) {
      if (getSchemaReader() != null) {
        this.keySerializer = SerializerDeserializerFactory.getVsonSerializer(getKeySchema());
        this.multiGetRequestSerializer = SerializerDeserializerFactory
            .getVsonSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
      } else {
        throw new VeniceClientException("SchemaReader is null when initializing serializer");
      }
    }
  }

  @Override
  public ComputeRequestBuilder<K> compute() {
    throw new VeniceClientException("'compute' is not supported in JSON store");
  }
}
