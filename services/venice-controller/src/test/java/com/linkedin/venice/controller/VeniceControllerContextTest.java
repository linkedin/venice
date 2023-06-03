package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;


public class VeniceControllerContextTest {
  @Test
  public void testVeniceServerContextCanSetDefaults() {
    VeniceControllerContext veniceControllerContext = new VeniceControllerContext.Builder().build();
    assertNotNull(veniceControllerContext.getMetricsRepository());
    assertNotNull(veniceControllerContext.getServiceDiscoveryAnnouncers());
    assertEquals(veniceControllerContext.getServiceDiscoveryAnnouncers(), Collections.emptyList());
  }

  @Test
  public void testVeniceServerContextCanSetValues() {

    List<VeniceProperties> propertiesList = Collections.emptyList();
    DynamicAccessController accessController = mock(DynamicAccessController.class);
    AuthorizerService authorizerService = mock(AuthorizerService.class);
    D2Client d2Client = mock(D2Client.class);
    ClientConfig routerClientConfig = mock(ClientConfig.class);
    ICProvider icProvider = mock(ICProvider.class);
    SupersetSchemaGenerator externalSupersetSchemaGenerator = mock(SupersetSchemaGenerator.class);
    PubSubClientsFactory pubSubClientsFactory = mock(PubSubClientsFactory.class);

    VeniceControllerContext veniceControllerContext =
        new VeniceControllerContext.Builder().setPropertiesList(propertiesList)
            .setAccessController(accessController)
            .setAuthorizerService(authorizerService)
            .setD2Client(d2Client)
            .setRouterClientConfig(routerClientConfig)
            .setIcProvider(icProvider)
            .setExternalSupersetSchemaGenerator(externalSupersetSchemaGenerator)
            .setPubSubClientsFactory(pubSubClientsFactory)
            .setMetricsRepository(null)
            .setServiceDiscoveryAnnouncers(null)
            .build();

    assertEquals(veniceControllerContext.getPropertiesList(), propertiesList);
    assertNull(veniceControllerContext.getMetricsRepository());
    assertNull(veniceControllerContext.getServiceDiscoveryAnnouncers());
    assertEquals(veniceControllerContext.getAccessController(), accessController);
    assertEquals(veniceControllerContext.getAuthorizerService(), authorizerService);
    assertEquals(veniceControllerContext.getD2Client(), d2Client);
    assertEquals(veniceControllerContext.getRouterClientConfig(), routerClientConfig);
    assertEquals(veniceControllerContext.getIcProvider(), icProvider);
    assertEquals(veniceControllerContext.getPubSubClientsFactory(), pubSubClientsFactory);
  }
}
