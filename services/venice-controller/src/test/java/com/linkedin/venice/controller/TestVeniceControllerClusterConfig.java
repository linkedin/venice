package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.CHILD_CLUSTER_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CHILD_DATA_CENTER_KAFKA_URL_PREFIX;
import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_D2;
import static com.linkedin.venice.ConfigKeys.CLUSTER_TO_SERVER_D2;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_DISABLED_ROUTES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_ID;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_INFO_PROCESSOR_NAME;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_INFO_SOURCES;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_CLOUD_PROVIDER;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_HELIX_REST_CUSTOMIZED_HEALTH_URL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PARENT_MODE;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_STORAGE_CLUSTER_HELIX_CLOUD_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.DEFAULT_PARTITION_SIZE;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_FABRIC_ALLOWLIST;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static com.linkedin.venice.PushJobCheckpoints.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.PushJobCheckpoints.QUOTA_EXCEEDED;
import static com.linkedin.venice.controller.VeniceControllerClusterConfig.parsePushJobUserErrorCheckpoints;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.model.CloudConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceControllerClusterConfig {
  private static final String DELIMITER = ",\\s*";
  private static final Set<String> REGION_ALLOW_LIST = Utils.setOf("dc1", "dc2");

  @Test
  public void canParseClusterMap() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.url.dc1", "http://host:1234, http://host:5678")
        .put("child.cluster.url.dc2", "http://host:1234, http://host:5678");

    Map<String, String> map = VeniceControllerClusterConfig.parseClusterMap(builder.build(), REGION_ALLOW_LIST);

    assertEquals(map.size(), 2);
    Assert.assertTrue(map.containsKey("dc1"));
    Assert.assertTrue(map.containsKey("dc2"));

    String[] uris = map.get("dc1").split(DELIMITER);
    assertEquals(uris[0], "http://host:1234");
    assertEquals(uris[1], "http://host:5678");
  }

  @Test
  public void canParseD2ClusterMap() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.d2.zkHost.dc1", "zkAddress1").put("child.cluster.d2.zkHost.dc2", "zkAddress2");

    Map<String, String> map = VeniceControllerClusterConfig.parseClusterMap(builder.build(), REGION_ALLOW_LIST, true);
    assertEquals(map.get("dc1").split(DELIMITER).length, 1);
    assertEquals(map.get("dc2").split(DELIMITER)[0], "zkAddress2");
  }

  @Test
  public void canParseBannedPaths() {
    PropertyBuilder builder = new PropertyBuilder();
    // Add some stuff. why not
    builder.put("child.cluster.d2.zkHost.dc1", "zkAddress1").put("child.cluster.d2.zkHost.dc2", "zkAddress2");

    // Add the list of disabled endpoints, '/' are optional, and will be ignored. Invalid values will be filtered
    builder.put(CONTROLLER_DISABLED_ROUTES, "request_topic, /discover_cluster, foo,bar");
    List<ControllerRoute> parsedRoutes = VeniceControllerClusterConfig
        .parseControllerRoutes(builder.build(), CONTROLLER_DISABLED_ROUTES, Collections.emptyList());

    // Make sure it looks right.
    assertEquals(parsedRoutes.size(), 2);
    Assert.assertTrue(parsedRoutes.contains(ControllerRoute.REQUEST_TOPIC));
    Assert.assertTrue(parsedRoutes.contains(ControllerRoute.CLUSTER_DISCOVERY));
  }

  @Test(expectedExceptions = VeniceException.class)
  public void emptyAllowlist() {
    PropertyBuilder build = new PropertyBuilder().put("child.cluster.url.dc1", "http://host:1234, http://host:5678")
        .put("child.cluster.url.dc2", "http://host:1234, http://host:5678");
    VeniceControllerClusterConfig.parseClusterMap(build.build(), Collections.emptySet());
  }

  @Test(expectedExceptions = VeniceException.class)
  public void nullAllowlist() {
    PropertyBuilder build = new PropertyBuilder().put("child.cluster.url.dc1", "http://host:1234, http://host:5678")
        .put("child.cluster.url.dc2", "http://host:1234, http://host:5678");
    VeniceControllerClusterConfig.parseClusterMap(build.build(), null);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingScheme() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.url.dc1", "host:1234");
    VeniceControllerClusterConfig.parseClusterMap(builder.build(), REGION_ALLOW_LIST);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void errOnMissingNodes() {
    PropertyBuilder builder = new PropertyBuilder();
    builder.put("child.cluster.url.dc1", "");
    VeniceControllerClusterConfig.parseClusterMap(builder.build(), REGION_ALLOW_LIST);
  }

  protected static Properties getBaseSingleRegionProperties(boolean includeMultiRegionConfig) {
    Properties props = TestUtils.getPropertiesForControllerConfig();
    String clusterName = props.getProperty(CLUSTER_NAME);
    props.put(LOCAL_REGION_NAME, "dc-0");
    props.put(ZOOKEEPER_ADDRESS, "zkAddress");
    props.put(KAFKA_BOOTSTRAP_SERVERS, "kafkaBootstrapServers");
    props.put(DEFAULT_PARTITION_SIZE, 10);
    props.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 16);
    props.put(CLUSTER_TO_D2, TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_d2")));
    props.put(
        CLUSTER_TO_SERVER_D2,
        TestUtils.getClusterToD2String(Collections.singletonMap(clusterName, "dummy_server_d2")));
    props.put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, true);
    props.put(ADMIN_HELIX_MESSAGING_CHANNEL_ENABLED, false);
    props.put(PARTICIPANT_MESSAGE_STORE_ENABLED, true);
    props.put(CONTROLLER_SYSTEM_SCHEMA_CLUSTER_NAME, clusterName);
    props.put(CONTROLLER_SSL_ENABLED, false);
    if (includeMultiRegionConfig) {
      props.put(MULTI_REGION, "false");
    }
    return props;
  }

  private Properties getBaseMultiRegionProperties(boolean includeMultiRegionConfig) {
    Properties props = getBaseSingleRegionProperties(false);
    props.put(NATIVE_REPLICATION_FABRIC_ALLOWLIST, "dc-0, dc-1, dc-parent");
    props.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + ".dc-0", "kafkaUrlDc0");
    props.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + ".dc-1", "kafkaUrlDc1");
    props.put(CHILD_DATA_CENTER_KAFKA_URL_PREFIX + ".dc-parent", "kafkaUrlDcParent");

    if (includeMultiRegionConfig) {
      props.put(MULTI_REGION, "true");
    }
    return props;
  }

  private Properties getBaseParentControllerProperties(boolean includeMultiRegionConfig) {
    Properties props = getBaseMultiRegionProperties(includeMultiRegionConfig);
    props.put(CONTROLLER_PARENT_MODE, "true");
    props.put(CHILD_CLUSTER_ALLOWLIST, "dc-0, dc-1");
    props.put(CHILD_CLUSTER_URL_PREFIX + "dc-0", "http://childControllerUrlDc0");
    props.put(CHILD_CLUSTER_URL_PREFIX + "dc-1", "http://childControllerUrlDc1");
    return props;
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testMultiRegionConfig(boolean explicitMultiRegionConfig) {
    Properties singleRegionProps = getBaseSingleRegionProperties(explicitMultiRegionConfig);
    VeniceControllerClusterConfig singleRegionConfig =
        new VeniceControllerClusterConfig(new VeniceProperties(singleRegionProps));
    Assert.assertFalse(singleRegionConfig.isMultiRegion());

    Properties multiRegionProps = getBaseMultiRegionProperties(explicitMultiRegionConfig);
    VeniceControllerClusterConfig multiRegionConfig =
        new VeniceControllerClusterConfig(new VeniceProperties(multiRegionProps));
    Assert.assertTrue(multiRegionConfig.isMultiRegion());

    Properties multiRegionPropsWithAaSourceRegion = getBaseMultiRegionProperties(explicitMultiRegionConfig);
    multiRegionPropsWithAaSourceRegion.put(ACTIVE_ACTIVE_REAL_TIME_SOURCE_FABRIC_LIST, "dc-0, dc-1");
    VeniceControllerClusterConfig multiRegionConfigWithAaSourceRegion =
        new VeniceControllerClusterConfig(new VeniceProperties(multiRegionPropsWithAaSourceRegion));
    Assert.assertTrue(multiRegionConfigWithAaSourceRegion.isMultiRegion());

    Properties parentControllerProps = getBaseParentControllerProperties(explicitMultiRegionConfig);
    VeniceControllerClusterConfig parentControllerConfig =
        new VeniceControllerClusterConfig(new VeniceProperties(parentControllerProps));
    Assert.assertTrue(parentControllerConfig.isMultiRegion());
  }

  @Test
  public void testParsePushJobUserErrorCheckpoints() {
    PushJobDetails pushJobDetails = mock(PushJobDetails.class);
    Map<CharSequence, CharSequence> pushJobConfigs = new HashMap<>();
    when(pushJobDetails.getPushJobConfigs()).thenReturn(pushJobConfigs);
    when(pushJobDetails.getPushJobLatestCheckpoint()).thenReturn(DVC_INGESTION_ERROR_OTHER.getValue());

    // valid
    Properties properties = new Properties();
    properties.put(PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR, "QUOTA_EXCEEDED,DVC_INGESTION_ERROR_OTHER");
    VeniceProperties controllerProps = new VeniceProperties(properties);
    Set<PushJobCheckpoints> expectedCustomUserErrorCheckpoints =
        new HashSet<>(Arrays.asList(QUOTA_EXCEEDED, DVC_INGESTION_ERROR_OTHER));
    assertEquals(expectedCustomUserErrorCheckpoints, parsePushJobUserErrorCheckpoints(controllerProps));

    // invalid cases: Should throw IllegalArgumentException
    Set<String> invalidCheckpointConfigs = new HashSet<>(
        Arrays.asList(
            "INVALID_CHECKPOINT",
            "[DVC_INGESTION_ERROR_OTHER",
            "DVC_INGESTION_ERROR_OTHER, RECORD_TOO_LARGE_FAILED]",
            "DVC_INGESTION_ERROR_OTHER, TEST",
            "-14"));
    for (String invalidCheckpointConfig: invalidCheckpointConfigs) {
      properties.put(PUSH_JOB_FAILURE_CHECKPOINTS_TO_DEFINE_USER_ERROR, invalidCheckpointConfig);
      VeniceProperties controllerPropsInvalid = new VeniceProperties(properties);
      assertThrows(IllegalArgumentException.class, () -> parsePushJobUserErrorCheckpoints(controllerPropsInvalid));
    }
  }

  @Test
  public void testHelixCloudConfig() {
    Properties baseProps = getBaseSingleRegionProperties(false);
    baseProps.setProperty(CONTROLLER_STORAGE_CLUSTER_HELIX_CLOUD_ENABLED, "true");

    UndefinedPropertyException e1 = expectThrows(
        UndefinedPropertyException.class,
        () -> new VeniceControllerClusterConfig(new VeniceProperties(baseProps)));
    assertTrue(e1.getMessage().contains("Missing required property '" + CONTROLLER_HELIX_CLOUD_PROVIDER + "'"));

    baseProps.setProperty(CONTROLLER_HELIX_CLOUD_PROVIDER, "invalidProvider");
    VeniceException e2 =
        expectThrows(VeniceException.class, () -> new VeniceControllerClusterConfig(new VeniceProperties(baseProps)));
    assertTrue(e2.getMessage().contains("Invalid Helix cloud provider"));

    baseProps.setProperty(CONTROLLER_HELIX_CLOUD_PROVIDER, CloudProvider.AZURE.name());
    VeniceControllerClusterConfig clusterConfig1 = new VeniceControllerClusterConfig(new VeniceProperties(baseProps));
    validateCloudConfig(clusterConfig1, CloudProvider.AZURE, null, null, null);

    CloudProvider cloudProvider = CloudProvider.CUSTOMIZED;
    String cloudId = "ABC";
    String processorName = "testProcessor";
    List<String> cloudInfoSources = Arrays.asList("source1", "source2");

    baseProps.setProperty(CONTROLLER_HELIX_CLOUD_PROVIDER, cloudProvider.name());
    baseProps.setProperty(CONTROLLER_HELIX_CLOUD_ID, cloudId);
    baseProps.setProperty(CONTROLLER_HELIX_CLOUD_INFO_PROCESSOR_NAME, processorName);
    baseProps.setProperty(CONTROLLER_HELIX_CLOUD_INFO_SOURCES, StringUtils.join(cloudInfoSources, ","));

    VeniceControllerClusterConfig clusterConfig2 = new VeniceControllerClusterConfig(new VeniceProperties(baseProps));
    validateCloudConfig(clusterConfig2, CloudProvider.CUSTOMIZED, cloudId, processorName, cloudInfoSources);
  }

  private void validateCloudConfig(
      VeniceControllerClusterConfig clusterConfig,
      CloudProvider cloudProvider,
      String cloudId,
      String processorName,
      List<String> cloudInfoSources) {
    CloudConfig cloudConfig = clusterConfig.getHelixCloudConfig();
    assertTrue(cloudConfig.isCloudEnabled());
    assertEquals(cloudConfig.getCloudProvider(), cloudProvider.name());
    assertEquals(cloudConfig.getCloudID(), cloudId);
    assertEquals(cloudConfig.getCloudInfoProcessorName(), processorName);
    assertEquals(cloudConfig.getCloudInfoSources(), cloudInfoSources);
  }

  @Test
  public void testHelixRestCustomizedHealthUrl() {
    Properties baseProps = getBaseSingleRegionProperties(false);

    String healthUrl = "http://localhost:8080/health";
    baseProps.setProperty(CONTROLLER_HELIX_REST_CUSTOMIZED_HEALTH_URL, healthUrl);

    VeniceControllerClusterConfig clusterConfig = new VeniceControllerClusterConfig(new VeniceProperties(baseProps));
    assertEquals(clusterConfig.getHelixRestCustomizedHealthUrl(), healthUrl);
  }
}
