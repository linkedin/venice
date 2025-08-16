package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import spark.Service;


public class AdminSparkServerTest {
  @Test
  public void testSparkJavaMaxThreadCount_withPositiveValue() throws Exception {
    // Create AdminSparkServer with a positive sparkJavaMaxThreadCount
    int port = 12345;
    Admin mockAdmin = Mockito.mock(Admin.class);
    MetricsRepository mockMetricsRepo = Mockito.mock(MetricsRepository.class);
    Sensor mockSensor = Mockito.mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepo).sensor(anyString(), any());
    Set<String> clusters = new HashSet<>();
    boolean enforceSSL = false;
    Optional<SSLConfig> sslConfig = Optional.empty();
    boolean checkReadMethodForKafka = false;
    Optional<DynamicAccessController> accessController = Optional.empty();
    List<ControllerRoute> disabledRoutes = Collections.emptyList();
    VeniceProperties jettyConfigOverrides = new VeniceProperties();
    boolean disableParentRequestTopicForStreamPushes = false;
    int sparkJavaMaxThreadCount = 10; // Positive thread count
    PubSubTopicRepository pubSubTopicRepository = Mockito.mock(PubSubTopicRepository.class);
    VeniceControllerRequestHandler requestHandler = Mockito.mock(VeniceControllerRequestHandler.class);

    AdminSparkServer server = new AdminSparkServer(
        port,
        mockAdmin,
        mockMetricsRepo,
        clusters,
        enforceSSL,
        sslConfig,
        checkReadMethodForKafka,
        accessController,
        disabledRoutes,
        jettyConfigOverrides,
        disableParentRequestTopicForStreamPushes,
        sparkJavaMaxThreadCount,
        pubSubTopicRepository,
        requestHandler);

    try {
      // Start the server
      server.start();
      // Check the thread counts in the Spark java service
      Service sparkJavaService = server.getSparkJavaService();
      Assert.assertNotNull(sparkJavaService, "Spark Java service should be initialized");
      Assert.assertTrue(sparkJavaService.activeThreadCount() >= 0, "The active thread count api needs to be available");

    } finally {
      // Clean up
      server.stop();
    }
  }
}
