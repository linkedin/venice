package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class TestVsonGenericStoreClientImpl {
  @Test
  public void testStart() {
    String storeName = "testStart";
    D2TransportClient transportClient = Mockito.mock(D2TransportClient.class);
    VsonGenericStoreClientImpl client = new VsonGenericStoreClientImpl(transportClient, ClientConfig.defaultVsonGenericClientConfig(storeName));

    // Mock the case that service discovery find the real d2 service, so create a new d2 transport client.
    D2ServiceDiscovery d2ServiceDiscovery = Mockito.mock(D2ServiceDiscovery.class);
    D2TransportClient newTransportClient = Mockito.mock(D2TransportClient.class);
    Mockito.doReturn(newTransportClient)
        .when(d2ServiceDiscovery)
        .getD2TransportClientForStore(Mockito.any(), Mockito.eq(storeName), Mockito.eq(false));
    client.setD2ServiceDiscovery(d2ServiceDiscovery);

    try {
      client.start();
    } catch (VeniceClientException e) {

    }

    // Use the original transport client to do the service discovery.
    Mockito.verify(transportClient, Mockito.never()).get(SchemaReader.TYPE_KEY_SCHEMA + "/" + storeName);
  }
}
