package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.ADMIN_TOPIC_SOURCE_REGION;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLIENT_ID_CONFIG;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Properties;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAdminConsumerService {
  @Test
  public void testMultipleAdminConsumerServiceWithSameMetricsRepo() {
    MetricsRepository metricsRepository = new MetricsRepository();

    String someClusterName = "clusterName";
    String adminTopicSourceRegion = "parent";

    VeniceProperties props = new PropertyBuilder().put(TestUtils.getPropertiesForControllerConfig())
        .put(ZOOKEEPER_ADDRESS, "localhost:2181")
        .put(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092")
        .put(DEFAULT_PARTITION_SIZE, "25GB")
        .put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "4096")
        .put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(Collections.singletonMap(someClusterName, "dummy_d2")))
        .put(
            CLUSTER_TO_SERVER_D2,
            TestUtils.getClusterToD2String(Collections.singletonMap(someClusterName, "dummy_server_d2")))
        .put(ADMIN_TOPIC_SOURCE_REGION, adminTopicSourceRegion)
        .put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, adminTopicSourceRegion)
        .put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + adminTopicSourceRegion, "blah")
        .put(CHILD_CLUSTER_ALLOWLIST, someClusterName)
        .put(SslUtils.getVeniceLocalSslProperties())
        .build();
    VeniceControllerClusterConfig controllerConfig = new VeniceControllerClusterConfig(props);

    PubSubConsumerAdapterFactory consumerFactory = mock(PubSubConsumerAdapterFactory.class);

    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    doReturn(mock(ZkClient.class)).when(admin).getZkClient();
    doReturn(mock(HelixAdapterSerializer.class)).when(admin).getAdapterSerializer();
    doReturn("localhost:1234").when(admin).getKafkaBootstrapServers(true);
    doReturn(true).when(admin).isSslToKafka();

    AdminConsumerService adminConsumerService1 = null;
    AdminConsumerService adminConsumerService2 = null;
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));

    try {
      adminConsumerService1 = new AdminConsumerService(
          admin,
          controllerConfig,
          metricsRepository,
          consumerFactory,
          pubSubTopicRepository,
          pubSubMessageDeserializer);

      /**
       * The creation of a second {@link AdminConsumerService} crashed after introducing a regression
       * which caused duplicate metrics getting registered.
       */
      try {
        adminConsumerService2 = new AdminConsumerService(
            admin,
            controllerConfig,
            metricsRepository,
            consumerFactory,
            pubSubTopicRepository,
            pubSubMessageDeserializer);
      } catch (Exception e) {
        Assert.fail("Creating a second " + AdminConsumerService.class.getSimpleName() + " should not fail", e);
      }
    } finally {
      Utils.closeQuietlyWithErrorLogged(adminConsumerService1);
      Utils.closeQuietlyWithErrorLogged(adminConsumerService2);
    }
  }

  @Test
  public void testCreateKafkaConsumerWithExpectedKafkaProperties() throws Exception {
    MetricsRepository metricsRepository = new MetricsRepository();

    String clusterName = "clusterName";
    String region = "dc-0";

    VeniceProperties props = new PropertyBuilder().put(TestUtils.getPropertiesForControllerConfig())
        .put(ZOOKEEPER_ADDRESS, "localhost:2181")
        .put(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092")
        .put(DEFAULT_PARTITION_SIZE, "25GB")
        .put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, "4096")
        .put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_d2")))
        .put(
            CLUSTER_TO_SERVER_D2,
            TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_server_d2")))
        .put(ADMIN_TOPIC_SOURCE_REGION, region)
        .put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, region)
        .put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + "." + region, "blah")
        .put(CHILD_CLUSTER_ALLOWLIST, clusterName)
        .put(LOCAL_REGION_NAME, region)
        .put(SslUtils.getVeniceLocalSslProperties())
        .build();
    VeniceControllerClusterConfig controllerConfig = new VeniceControllerClusterConfig(props);

    PubSubConsumerAdapterFactory consumerFactory = mock(PubSubConsumerAdapterFactory.class);
    PubSubConsumerAdapter mockConsumer = mock(PubSubConsumerAdapter.class);
    when(consumerFactory.create(any(), eq(false), any(), eq(clusterName))).thenReturn(mockConsumer);

    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    doReturn(mock(ZkClient.class)).when(admin).getZkClient();
    doReturn(mock(HelixAdapterSerializer.class)).when(admin).getAdapterSerializer();
    doReturn("localhost:1234").when(admin).getKafkaBootstrapServers(true);
    doReturn(false).when(admin).isSslToKafka();
    VeniceProperties veniceProperties = mock(VeniceProperties.class);
    Properties pubSubProperties = new Properties();
    when(veniceProperties.toProperties()).thenReturn(pubSubProperties);
    when(admin.getPubSubSSLProperties(anyString())).thenReturn(veniceProperties);

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubMessageDeserializer deserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));

    AdminConsumerService service = new AdminConsumerService(
        admin,
        controllerConfig,
        metricsRepository,
        consumerFactory,
        pubSubTopicRepository,
        deserializer);

    // Access private createKafkaConsumer via reflection
    Method method = AdminConsumerService.class.getDeclaredMethod("createKafkaConsumer", String.class);
    method.setAccessible(true);
    PubSubConsumerAdapter result = (PubSubConsumerAdapter) method.invoke(service, clusterName);

    assertNotNull(result);
    verify(admin).getPubSubSSLProperties(anyString());
    verify(consumerFactory).create(any(VeniceProperties.class), eq(false), eq(deserializer), eq(clusterName));
    String clientId = pubSubProperties.getProperty(KAFKA_CLIENT_ID_CONFIG);
    assertNotNull(clientId, "Client ID should not be null");
    assertTrue(clientId.contains(controllerConfig.getLogContext().toString()), "Got: " + clientId);
  }
}
