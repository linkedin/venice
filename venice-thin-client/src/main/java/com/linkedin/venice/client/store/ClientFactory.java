package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.map.ObjectMapper;


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
      internalClient = new VsonGenericStoreClientImpl<>(transportClient, clientConfig.getStoreName());
    } else {
      internalClient = new AvroGenericStoreClientImpl<>(transportClient, clientConfig.getStoreName());
    }

    StatTrackingStoreClient<K, V> client;
    if (clientConfig.getMetricsRepository() != null) {
      client = new StatTrackingStoreClient<>(internalClient, clientConfig.getMetricsRepository());
    } else {
      client = new StatTrackingStoreClient<>(internalClient);
    }

    return client;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartSpecificAvroClient(ClientConfig<V> clientConfig) {
    AvroSpecificStoreClient<K, V> client = getSpecificAvroClient(clientConfig);
    client.start();
    return client;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getSpecificAvroClient(ClientConfig<V> clientConfig) {
    TransportClient transportClient = getTransportClient(clientConfig);
    InternalAvroStoreClient<K, V> avroClient =
        new AvroSpecificStoreClientImpl<>(transportClient, clientConfig.getStoreName(), clientConfig.getSpecificValueClass());

    SpecificStatTrackingStoreClient<K, V> client;
    if (clientConfig.getMetricsRepository() != null) {
      client = new SpecificStatTrackingStoreClient<>(avroClient, clientConfig.getMetricsRepository());
    } else {
      client = new SpecificStatTrackingStoreClient<>(avroClient);
    }

    return client;
  }

  private static D2TransportClient generateTransportClient(ClientConfig clientConfig){
    String d2ServiceName = clientConfig.getD2ServiceName();

    if (clientConfig.getD2Client() != null) {
      return new D2TransportClient(d2ServiceName, clientConfig.getD2Client());
    }

    return new D2TransportClient(clientConfig.getVeniceURL(),
        d2ServiceName,
        clientConfig.getD2BasePath(),
        clientConfig.getD2ZkTimeout());
  }

  private static TransportClient getTransportClient(ClientConfig clientConfig) {
    String bootstrapUrl = clientConfig.getVeniceURL();

    if (clientConfig.isD2Routing()) {
      if (clientConfig.getD2ServiceName() == null ) {
        throw new VeniceClientException("D2 Server name can't be null");
      }
      return generateTransportClient(clientConfig);
    } else if (clientConfig.isHttps()){
      if (clientConfig.getSslEngineComponentFactory() == null) {
        throw new VeniceClientException("Must use SSL factory method for client to communicate with https");
      }

      return new HttpsTransportClient(bootstrapUrl, clientConfig.getSslEngineComponentFactory());
    } else {
      return new HttpTransportClient(bootstrapUrl);
    }
  }
}