package com.linkedin.venice.client.store;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.HttpsTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import java.util.Optional;
import org.apache.avro.specific.SpecificRecord;

public class AvroStoreClientFactory {
  public static final String HTTP_PREFIX = "http://";
  public static final String HTTPS_PREFIX = "https://";
  public static final String D2_PREFIX = "d2://";

  // TODO: come up with some new interfaces, which could be used by Offspring MP

  // TODO: Add ClientConfig to configure transport client, such as timeout, thread number, ...
  // TODO: Construct StoreClient by D2 url along with a couple of D2 related config.
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartAvroGenericStoreClient(String url, String storeName)
      throws VeniceClientException {
    TransportClient transportClient = getTransportClient(url, Optional.empty());
    AvroGenericStoreClientImpl<K, V> avroStoreClient = new AvroGenericStoreClientImpl(transportClient, storeName);
    DelegatingStoreClient<K, V> statTrackingStoreClient = new StatTrackingStoreClient<K, V>(avroStoreClient);
    statTrackingStoreClient.start();
    return statTrackingStoreClient;
  }

  public static <K, V> AvroGenericStoreClient<K, V> getAndStartAvroGenericSslStoreClient(String url, String storeName, SSLEngineComponentFactory sslFactory) {
    TransportClient transportClient = getTransportClient(url, Optional.of(sslFactory));
    AvroGenericStoreClientImpl<K, V> avroStoreClient = new AvroGenericStoreClientImpl(transportClient, storeName);
    DelegatingStoreClient<K, V> statTrackingStoreClient = new StatTrackingStoreClient<K, V>(avroStoreClient);
    statTrackingStoreClient.start();
    return statTrackingStoreClient;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartAvroSpecificStoreClient(
    String url, String storeName, Class<V> valueClass) throws VeniceClientException {
    TransportClient transportClient = getTransportClient(url, Optional.empty());
    AvroSpecificStoreClientImpl<K, V> avroStoreClient = new AvroSpecificStoreClientImpl(transportClient, storeName, valueClass);
    SpecificStatTrackingStoreClient<K, V> statTrackingStoreClient = new SpecificStatTrackingStoreClient<>(avroStoreClient);
    statTrackingStoreClient.start();
    return statTrackingStoreClient;
  }

  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartAvroSpecificSslStoreClient(
      String url, String storeName, Class<V> valueClass, SSLEngineComponentFactory sslFactory) throws VeniceClientException {
    TransportClient transportClient = getTransportClient(url, Optional.of(sslFactory));
    AvroSpecificStoreClientImpl<K, V> avroStoreClient = new AvroSpecificStoreClientImpl(transportClient, storeName, valueClass);
    SpecificStatTrackingStoreClient<K, V> statTrackingStoreClient = new SpecificStatTrackingStoreClient<>(avroStoreClient);
    statTrackingStoreClient.start();
    return statTrackingStoreClient;
  }

  private static TransportClient getTransportClient(String url, Optional<SSLEngineComponentFactory> sslFactory) throws VeniceClientException {
    if (url.startsWith(HTTP_PREFIX)) {
      return new HttpTransportClient(url);
    } else if (url.startsWith(HTTPS_PREFIX)){
      if (sslFactory.isPresent()){
        return new HttpsTransportClient(url, sslFactory.get());
      } else {
        throw new VeniceClientException("Must use SSL factory method for client to communicate with https url: " + url);
      }
    } else if (url.startsWith(D2_PREFIX)) {
      throw new VeniceClientException("Initializing D2 StoreClient by url is not supported yet");
    }
    throw new VeniceClientException("Unknown url: " + url);
  }

  // Temporary constructors for D2Client
  public static <K, V> AvroGenericStoreClient<K, V> getAndStartAvroGenericStoreClient(String d2ServiceName,
                                                                                D2Client d2Client,
                                                                                String storeName) throws VeniceClientException {
    TransportClient d2TransportClient = new D2TransportClient(d2ServiceName, d2Client);
    AvroGenericStoreClientImpl<K, V> avroStoreClient = new AvroGenericStoreClientImpl(d2TransportClient, storeName);
    DelegatingStoreClient<K, V> statTrackingStoreClient = new StatTrackingStoreClient<>(avroStoreClient);
    statTrackingStoreClient.start();
    return statTrackingStoreClient;
  }

  // Temporary constructors for D2Client
  public static <K, V extends SpecificRecord> AvroSpecificStoreClient<K, V> getAndStartAvroSpecificStoreClient(
    String d2ServiceName, D2Client d2Client, String storeName, Class<V> valueClass) throws VeniceClientException {
    TransportClient d2TransportClient = new D2TransportClient(d2ServiceName, d2Client);
    AvroSpecificStoreClientImpl<K, V> avroStoreClient = new AvroSpecificStoreClientImpl(d2TransportClient, storeName, valueClass);
    SpecificStatTrackingStoreClient<K, V> statTrackingStoreClient = new SpecificStatTrackingStoreClient<>(avroStoreClient);
    statTrackingStoreClient.start();
    return statTrackingStoreClient;
  }
}
