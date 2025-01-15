package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import org.testng.annotations.Test;


public class ControllerRequestHandlerDependenciesTest {
  @Test
  public void testBuilderWithAllFieldsSet() {
    Admin admin = mock(Admin.class);
    SSLConfig sslConfig = mock(SSLConfig.class);
    DynamicAccessController accessController = mock(DynamicAccessController.class);
    PubSubTopicRepository pubSubTopicRepository = mock(PubSubTopicRepository.class);
    MetricsRepository metricsRepository = mock(MetricsRepository.class);
    VeniceProperties veniceProperties = mock(VeniceProperties.class);
    ControllerRoute route = ControllerRoute.STORE;

    ControllerRequestHandlerDependencies dependencies =
        new ControllerRequestHandlerDependencies.Builder().setAdmin(admin)
            .setClusters(Collections.singleton("testCluster"))
            .setEnforceSSL(true)
            .setSslEnabled(true)
            .setCheckReadMethodForKafka(true)
            .setSslConfig(sslConfig)
            .setAccessController(accessController)
            .setDisabledRoutes(Collections.singletonList(route))
            .setDisableParentRequestTopicForStreamPushes(true)
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setMetricsRepository(metricsRepository)
            .setVeniceProperties(veniceProperties)
            .build();

    assertEquals(dependencies.getAdmin(), admin);
    assertEquals(dependencies.getClusters(), Collections.singleton("testCluster"));
    assertTrue(dependencies.isEnforceSSL());
    assertTrue(dependencies.isSslEnabled());
    assertTrue(dependencies.isCheckReadMethodForKafka());
    assertEquals(dependencies.getSslConfig(), sslConfig);
    assertEquals(dependencies.getAccessController(), accessController);
    assertEquals(dependencies.getDisabledRoutes(), Collections.singletonList(route));
    assertTrue(dependencies.isDisableParentRequestTopicForStreamPushes());
    assertEquals(dependencies.getPubSubTopicRepository(), pubSubTopicRepository);
    assertEquals(dependencies.getMetricsRepository(), metricsRepository);
    assertEquals(dependencies.getVeniceProperties(), veniceProperties);
  }

  @Test
  public void testBuilderWithDefaultPubSubTopicRepository() {
    Admin admin = mock(Admin.class);

    ControllerRequestHandlerDependencies dependencies =
        new ControllerRequestHandlerDependencies.Builder().setAdmin(admin)
            .setClusters(Collections.singleton("testCluster"))
            .build();

    assertNotNull(dependencies.getPubSubTopicRepository());
  }

  @Test
  public void testBuilderWithMissingAdmin() {
    // Expect exception when admin is missing
    IllegalArgumentException exception = expectThrows(
        IllegalArgumentException.class,
        () -> new ControllerRequestHandlerDependencies.Builder().setClusters(Collections.singleton("testCluster"))
            .build());

    assertEquals(exception.getMessage(), "admin is mandatory dependencies for VeniceControllerRequestHandler");
  }

  @Test
  public void testDefaultValues() {
    Admin admin = mock(Admin.class);

    ControllerRequestHandlerDependencies dependencies =
        new ControllerRequestHandlerDependencies.Builder().setAdmin(admin)
            .setClusters(Collections.singleton("testCluster"))
            .build();

    assertFalse(dependencies.isEnforceSSL());
    assertFalse(dependencies.isSslEnabled());
    assertFalse(dependencies.isCheckReadMethodForKafka());
    assertNull(dependencies.getSslConfig());
    assertNull(dependencies.getAccessController());
    assertTrue(dependencies.getDisabledRoutes().isEmpty());
    assertFalse(dependencies.isDisableParentRequestTopicForStreamPushes());
    assertNull(dependencies.getMetricsRepository());
    assertNull(dependencies.getVeniceProperties());
  }
}
