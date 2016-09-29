package com.linkedin.venice.client.store;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import org.apache.avro.specific.SpecificRecord;

public class AvroStoreClientFactory {
  public static final String HTTP_PREFIX = "http://";
  public static final String D2_PREFIX = "d2://";

  // TODO: Add ClientConfig to configure transport client, such as timeout, thread number, ...
  // TODO: Construct StoreClient by D2 url along with a couple of D2 related config.
  public static <V> AvroGenericStoreClient<V> getAndStartAvroGenericStoreClient(String url, String storeName)
      throws VeniceClientException {
    TransportClient<V> transportClient = getTransportClient(url);
    AvroGenericStoreClientImpl<V> client = new AvroGenericStoreClientImpl(transportClient, storeName);
    client.start();
    return client;
  }

  public static <V extends SpecificRecord> AvroSpecificStoreClient<V> getAndStartAvroSpecificStoreClient(
    String url, String storeName, Class<V> valueClass) throws VeniceClientException {
    TransportClient<V> transportClient = getTransportClient(url);
    AvroSpecificStoreClientImpl<V> client = new AvroSpecificStoreClientImpl(transportClient, storeName, valueClass);
    client.start();
    return client;
  }

  private static <V> TransportClient<V> getTransportClient(String url) throws VeniceClientException {
    if (url.startsWith(HTTP_PREFIX)) {
      return new HttpTransportClient<>(url);
    }
    if (url.startsWith(D2_PREFIX)) {
      throw new VeniceClientException("Initializing D2 StoreClient by url is not supported yet");
    }
    throw new VeniceClientException("Unknown url: " + url);
  }

  // Temporary constructors for D2Client
  public static <V> AvroGenericStoreClient<V> getAndStartAvroGenericStoreClient(String d2ServiceName,
                                                                                D2Client d2Client,
                                                                                String storeName) throws VeniceClientException {
    TransportClient<V> d2TransportClient = new D2TransportClient<V>(d2ServiceName, d2Client);
    AvroGenericStoreClientImpl<V> client = new AvroGenericStoreClientImpl(d2TransportClient, storeName);
    client.start();
    return client;
  }

  // Temporary constructors for D2Client
  public static <V extends SpecificRecord> AvroSpecificStoreClient<V> getAndStartAvroSpecificStoreClient(
    String d2ServiceName, D2Client d2Client, String storeName, Class<V> valueClass) throws VeniceClientException {
    TransportClient<V> d2TransportClient = new D2TransportClient<V>(d2ServiceName, d2Client);
    AvroSpecificStoreClientImpl<V> client = new AvroSpecificStoreClientImpl(d2TransportClient, storeName, valueClass);
    client.start();
    return client;
  }
}
