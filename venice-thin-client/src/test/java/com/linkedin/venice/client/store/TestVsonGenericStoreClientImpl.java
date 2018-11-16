package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class TestVsonGenericStoreClientImpl {
  @Test
  public void testInit() {
    String storeName = "testInit";
    D2TransportClient transportClient = Mockito.mock(D2TransportClient.class);
    VsonGenericStoreClientImpl client = new VsonGenericStoreClientImpl(transportClient, ClientConfig.defaultVsonGenericClientConfig(storeName));

    // Mock the case that service discovery find the real d2 service, so create a new d2 transport client.
    D2ServiceDiscovery d2ServiceDiscovery = Mockito.mock(D2ServiceDiscovery.class);
    D2TransportClient newTransportClient = Mockito.mock(D2TransportClient.class);
    Mockito.doReturn(newTransportClient)
        .when(d2ServiceDiscovery)
        .getD2TransportClientForStore(Mockito.any(), Mockito.eq(storeName));
    client.setD2ServiceDiscovery(d2ServiceDiscovery);
    client.start();

    try {
      client.init();
    } catch (VeniceClientException e) {

    }

    // Use the original transport client to do the service discovery.
    Mockito.verify(newTransportClient, Mockito.atLeastOnce()).get(SchemaReader.TYPE_KEY_SCHEMA + "/" + storeName);
    Mockito.verify(transportClient, Mockito.never()).get(SchemaReader.TYPE_KEY_SCHEMA + "/" + storeName);
  }
}
