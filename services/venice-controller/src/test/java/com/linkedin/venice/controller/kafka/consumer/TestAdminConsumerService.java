package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_ZK_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_WHITELIST;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAdminConsumerService {
  @Test
  public void testMultipleAdminConsumerServiceWithSameMetricsRepo() throws IOException {
    MetricsRepository metricsRepository = new MetricsRepository();

    String someClusterName = "clusterName";
    String adminTopicSourceRegion = "parent";

    VeniceProperties props = new PropertyBuilder().put(TestUtils.getPropertiesForControllerConfig())
        .put(CLUSTER_TO_D2, TestUtils.getClusterToDefaultD2String(someClusterName))
        .put(ADMIN_TOPIC_REMOTE_CONSUMPTION_ENABLED, true)
        .put(ADMIN_TOPIC_SOURCE_REGION, adminTopicSourceRegion)
        .put(NATIVE_REPLICATION_FABRIC_WHITELIST, adminTopicSourceRegion)
        .put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + adminTopicSourceRegion, "blah")
        .put(CHILD_DATA_CENTER_KAFKA_ZK_PREFIX + "." + adminTopicSourceRegion, "blah")
        .put(CHILD_CLUSTER_ALLOWLIST, someClusterName)
        .put(SslUtils.getVeniceLocalSslProperties())
        .build();
    VeniceControllerConfig controllerConfig = new VeniceControllerConfig(props);

    ControllerKafkaClientFactory consumerFactory = new ControllerKafkaClientFactory(
        controllerConfig,
        Optional.of(new KafkaClientFactory.MetricsParameters("test", metricsRepository)));

    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    doReturn(mock(ZkClient.class)).when(admin).getZkClient();
    doReturn(mock(HelixAdapterSerializer.class)).when(admin).getAdapterSerializer();
    doReturn(consumerFactory).when(admin).getVeniceConsumerFactory();

    AdminConsumerService adminConsumerService1 = null;
    AdminConsumerService adminConsumerService2 = null;
    try {
      adminConsumerService1 =
          new AdminConsumerService("cluster1", admin, controllerConfig, metricsRepository, Optional.empty());

      /**
       * The creation of a second {@link AdminConsumerService} crashed after introducing a regression
       * which caused duplicate metrics getting registered.
       */
      try {
        adminConsumerService2 =
            new AdminConsumerService("cluster2", admin, controllerConfig, metricsRepository, Optional.empty());
      } catch (Exception e) {
        Assert.fail("Creating a second " + AdminConsumerService.class.getSimpleName() + " should not fail", e);
      }
    } finally {
      Utils.closeQuietlyWithErrorLogged(adminConsumerService1);
      Utils.closeQuietlyWithErrorLogged(adminConsumerService2);
    }
  }
}
