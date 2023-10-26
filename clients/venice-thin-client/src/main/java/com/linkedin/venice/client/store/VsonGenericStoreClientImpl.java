package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.util.Optional;
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
  protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
    return new VsonGenericStoreClientImpl<K, V>(
        getTransportClient().getCopyIfNotUsableInCallback(),
        false,
        ClientConfig.defaultVsonGenericClientConfig(getStoreName()));
  }

  @Override
  protected RecordDeserializer<V> getDeserializerFromFactory(Schema writer, Schema reader) {
    return SerializerDeserializerFactory.getVsonDeserializer(writer, reader);
  }

  @Override
  protected RecordSerializer<K> createKeySerializer() {
    return SerializerDeserializerFactory.getVsonSerializer(getKeySchema());
  }

  @Override
  public ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      AvroGenericReadComputeStoreClient computeStoreClient) throws VeniceClientException {
    throw new VeniceClientException("'compute' is not supported in JSON store");
  }
}
