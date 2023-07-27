package com.linkedin.venice.server;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.service.ICProvider;
import java.util.Collections;
import org.testng.annotations.Test;


public class VeniceServerContextTest {
  @Test
  public void testVeniceServerContextCanSetDefaults() {
    VeniceServerContext veniceServerContext = new VeniceServerContext.Builder().build();
    assertNotNull(veniceServerContext.getMetricsRepository());
    assertNotNull(veniceServerContext.getServiceDiscoveryAnnouncers());
    assertEquals(veniceServerContext.getServiceDiscoveryAnnouncers(), Collections.emptyList());
  }

  @Test
  public void testVeniceServerContextCanSetValues() {
    VeniceConfigLoader veniceConfigLoaderMock = mock(VeniceConfigLoader.class);
    SSLFactory sslFactoryMock = mock(SSLFactory.class);
    StaticAccessController routerAccessControllerMock = mock(StaticAccessController.class);
    DynamicAccessController storeAccessControllerMock = mock(DynamicAccessController.class);
    ClientConfig clientConfigForConsumerMock = mock(ClientConfig.class);
    ICProvider icProviderMock = mock(ICProvider.class);

    VeniceServerContext veniceServerContext =
        new VeniceServerContext.Builder().setVeniceConfigLoader(veniceConfigLoaderMock)
            .setSslFactory(sslFactoryMock)
            .setRouterAccessController(routerAccessControllerMock)
            .setStoreAccessController(storeAccessControllerMock)
            .setClientConfigForConsumer(clientConfigForConsumerMock)
            .setIcProvider(icProviderMock)
            .setMetricsRepository(null)
            .setServiceDiscoveryAnnouncers(null)
            .build();

    assertEquals(veniceServerContext.getVeniceConfigLoader(), veniceConfigLoaderMock);
    assertNull(veniceServerContext.getMetricsRepository());
    assertNull(veniceServerContext.getServiceDiscoveryAnnouncers());
    assertEquals(veniceServerContext.getSslFactory(), sslFactoryMock);
    assertEquals(veniceServerContext.getRouterAccessController(), routerAccessControllerMock);
    assertEquals(veniceServerContext.getStoreAccessController(), storeAccessControllerMock);
    assertEquals(veniceServerContext.getClientConfigForConsumer(), clientConfigForConsumerMock);
    assertEquals(veniceServerContext.getIcProvider(), icProviderMock);
  }
}
