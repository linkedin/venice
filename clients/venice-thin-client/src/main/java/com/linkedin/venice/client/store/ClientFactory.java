package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.schema.RouterBasedStoreSchemaFetcher;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.service.ICProvider;
import java.util.Optional;
import org.apache.avro.specific.SpecificRecord;


public class ClientFactory {
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartGenericAvroClient(ClientConfig clientConfig) {
    AvroGenericStoreClient<K, V> client = getGenericAvroClient(clientConfig);
    client.start();
    return client;
  }

  public static <K, V> AvroGenericStoreClient<K, V> getGenericAvroClient(ClientConfig clientConfig) {
    TransportClient transportClient = getTransportClient(clientConfig);
    InternalAvroStoreClient<K, V> internalClient;

    if (clientConfig.isVsonClient()) {
      internalClient = new VsonGenericStoreClientImpl<>(transportClient, clientConfig);
    } else {
      if (clientConfig.isUseBlackHoleDeserializer()) {
        internalClient = new AvroBlackHoleResponseStoreClientImpl<>(transportClient, clientConfig);
      } else {
        internalClient = new AvroGenericStoreClientImpl<>(transportClient, clientConfig);
      }
    }

    StatTrackingStoreClient<K, V> client = new StatTrackingStoreClient<>(internalClient, clientConfig);
    if (clientConfig.isRetryOnRouterErrorEnabled() || clientConfig.isRetryOnAllErrorsEnabled()) {
      return new RetriableStoreClient<>(client, clientConfig);
    }
    return client;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartSpecificAvroClient(
      ClientConfig<V> clientConfig) {
    AvroSpecificStoreClient<K, V> client = getSpecificAvroClient(clientConfig);
    client.start();
    return client;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getSpecificAvroClient(
      ClientConfig<V> clientConfig) {
    TransportClient transportClient = getTransportClient(clientConfig);
    InternalAvroStoreClient<K, V> avroClient = new AvroSpecificStoreClientImpl<>(transportClient, clientConfig);

    SpecificStatTrackingStoreClient<K, V> client = new SpecificStatTrackingStoreClient<>(avroClient, clientConfig);

    if (clientConfig.isRetryOnRouterErrorEnabled() || clientConfig.isRetryOnAllErrorsEnabled()) {
      return new SpecificRetriableStoreClient<>(client, clientConfig);
    }

    return client;
  }

  public static <K, V> AvroGenericStoreClient<K, V> getAndStartAvroClient(ClientConfig clientConfig) {
    if (clientConfig.isSpecificClient()) {
      return ClientFactory.getAndStartSpecificAvroClient(clientConfig);
    } else {
      return ClientFactory.getAndStartGenericAvroClient(clientConfig);
    }
  }

  public static SchemaReader getSchemaReader(ClientConfig clientConfig) {
    return getSchemaReader(clientConfig, null);
  }

  public static SchemaReader getSchemaReader(ClientConfig clientConfig, ICProvider icProvider) {
    /**
     * N.B.: instead of returning a new {@link SchemaReader}, we could instead return
     * {@link AbstractAvroStoreClient#getSchemaReader()}, but then the calling code would
     * have no handle on the original client, and therefore it would leak with no ability
     * to close it. In order to alleviate that risk, we instead construct a client with
     * needSchemaReader == false, and pass that client to a new {@link SchemaReader}, which
     * is the same as what would happen inside {@link AbstractAvroStoreClient#start()}.
     *
     * Closing this {@link SchemaReader} instance will also close the underlying client.
     */
    return new RouterBackedSchemaReader(
        () -> new AvroGenericStoreClientImpl<>(getTransportClient(clientConfig), false, clientConfig),
        Optional.empty(),
        clientConfig.getPreferredSchemaFilter(),
        clientConfig.getSchemaRefreshPeriod(),
        icProvider);
  }

  public static StoreSchemaFetcher createStoreSchemaFetcher(ClientConfig clientConfig) {
    return new RouterBasedStoreSchemaFetcher(
        new AvroGenericStoreClientImpl<>(getTransportClient(clientConfig), false, clientConfig));
  }

  private static D2TransportClient generateTransportClient(ClientConfig clientConfig) {
    String d2ServiceName = clientConfig.getD2ServiceName();

    if (clientConfig.getD2Client() != null) {
      return new D2TransportClient(d2ServiceName, clientConfig.getD2Client());
    }

    return new D2TransportClient(d2ServiceName, clientConfig);
  }

  public static TransportClient getTransportClient(ClientConfig clientConfig) {
    String bootstrapUrl = clientConfig.getVeniceURL();

    if (clientConfig.isD2Routing()) {
      if (clientConfig.getD2ServiceName() == null) {
        throw new VeniceClientException("D2 Server name can't be null");
      }
      return generateTransportClient(clientConfig);
    } else if (clientConfig.isHttps()) {
      if (clientConfig.getSslFactory() == null) {
        throw new VeniceClientException("Must use SSL factory method for client to communicate with https");
      }

      return new HttpsTransportClient(
          bootstrapUrl,
          clientConfig.getMaxConnectionsTotal(),
          clientConfig.getMaxConnectionsPerRoute(),
          clientConfig.isHttpClient5Http2Enabled(),
          clientConfig.getSslFactory());
    } else {
      return new HttpTransportClient(
          bootstrapUrl,
          clientConfig.getMaxConnectionsTotal(),
          clientConfig.getMaxConnectionsPerRoute());
    }
  }
}
