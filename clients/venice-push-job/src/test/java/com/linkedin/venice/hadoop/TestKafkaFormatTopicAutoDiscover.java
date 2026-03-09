package com.linkedin.venice.hadoop;

import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.RepushInfoResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestKafkaFormatTopicAutoDiscover {
  private static final String JOB_ID = "some-job-ID";
  private static final String STORE_NAME = "store-name";
  private static final String STORE_NAME_2 = "store-name-2";

  @Test(expectedExceptions = UndefinedPropertyException.class, expectedExceptionsMessageRegExp = "Missing required property 'venice.store.name'.")
  public void testMissingStoreNameInConfig() {
    new VenicePushJob(JOB_ID, getJobProperties(Collections.emptyMap())).close();
  }

  @Test
  public void testNoUserProvidedTopicNameAndSingleColoVersion() {
    final int singleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = Collections.emptyMap(); // Single colo
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, singleColoCurrentVersion);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    RepushInfoResponse repushInfo = getMockRepushResponse(1);
    when(controllerClient.getRepushInfo(STORE_NAME, Optional.empty())).thenReturn(repushInfo);
    configureClusterDiscoveryControllerClient(controllerClient);

    Map<String, String> overrideProperties = Collections.singletonMap(VENICE_STORE_NAME_PROP, STORE_NAME);
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      String expectedTopicName = Version.composeKafkaTopic(STORE_NAME, singleColoCurrentVersion);
      Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, expectedTopicName);
    }
  }

  @Test
  public void testUserProvidedEpochRewind() {
    final int singleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = Collections.emptyMap();
    StoreResponse storeResponse =
        getMockHybridStoreResponse(coloToVersionMap, singleColoCurrentVersion, BufferReplayPolicy.REWIND_FROM_SOP);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    RepushInfoResponse repushInfo = getMockRepushResponse(1);
    when(controllerClient.getRepushInfo(STORE_NAME, Optional.empty())).thenReturn(repushInfo);
    configureClusterDiscoveryControllerClient(controllerClient);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE, "1637016606");
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.validateRemoteHybridSettings();
    }
  }

  @Test
  public void testUserProvidedEpochRewindWithInvalidRemotePolicy() {
    final int singleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = Collections.emptyMap();
    StoreResponse storeResponse =
        getMockHybridStoreResponse(coloToVersionMap, singleColoCurrentVersion, BufferReplayPolicy.REWIND_FROM_EOP);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    RepushInfoResponse repushInfo = getMockRepushResponse(1);
    when(controllerClient.getRepushInfo(STORE_NAME, Optional.empty())).thenReturn(repushInfo);
    configureClusterDiscoveryControllerClient(controllerClient);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE, "1637016606");
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      venicePushJob.setControllerClient(controllerClient);
      Assert.assertThrows(venicePushJob::validateRemoteHybridSettings);
    }
  }

  @Test
  public void testNoUserProvidedTopicNameAndMultiColoVersion() {
    final int multipleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    RepushInfoResponse repushInfo = getMockRepushResponse(1);
    when(controllerClient.getRepushInfo(STORE_NAME, Optional.empty())).thenReturn(repushInfo);
    configureClusterDiscoveryControllerClient(controllerClient);

    Map<String, String> overrideProperties = Collections.singletonMap(VENICE_STORE_NAME_PROP, STORE_NAME);
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      String expectedTopicName = Version.composeKafkaTopic(STORE_NAME, multipleColoCurrentVersion);
      Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, expectedTopicName);
    }
  }

  @Test
  public void testNoUserProvidedTopicNameAndMultiColoVersionMismatch() {
    // Mismatched versions below
    final int multipleColoCurrentVersion1 = 1;
    final int multipleColoCurrentVersion2 = 2;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion2);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    RepushInfoResponse repushInfo = getMockRepushResponse(multipleColoCurrentVersion2);
    when(controllerClient.getRepushInfo(STORE_NAME, Optional.empty())).thenReturn(repushInfo);
    configureClusterDiscoveryControllerClient(controllerClient);
    Map<String, String> overrideProperties = Collections.singletonMap(VENICE_STORE_NAME_PROP, STORE_NAME);
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      String expectedTopicName = Version.composeKafkaTopic(STORE_NAME, multipleColoCurrentVersion2);
      Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, expectedTopicName);
    }
  }

  @Test
  public void testUserProvidedTopicNameAndMultiColoVersionMismatch() {
    // Mismatched versions below
    final int multipleColoCurrentVersion1 = 1;
    final int multipleColoCurrentVersion2 = 2;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion1);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion2);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    configureClusterDiscoveryControllerClient(controllerClient);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME, 3);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, userProvidedTopicName);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store user-provided name mismatch with the derived store name.*")
  public void testUserProvidedTopicNameNotValid() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    StoreResponse storeResponse = getMockStoreResponse(Collections.emptyMap(), -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    configureClusterDiscoveryControllerClient(controllerClient);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME_2, 3);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
    }
  }

  @Test
  public void testUserProvidedTopicNameMatchDiscoveredTopicName() {
    final int multipleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    configureClusterDiscoveryControllerClient(controllerClient);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME, multipleColoCurrentVersion);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, userProvidedTopicName);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store user-provided name mismatch with the derived store name.*")
  public void testUserProvidedTopicNameMismatchDiscoveredTopicName() {
    final int multipleColoCurrentVersion = 1;
    ControllerClient controllerClient = mock(ControllerClient.class);
    Map<String, Integer> coloToVersionMap = new HashMap<>(3);
    coloToVersionMap.put("colo-0", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-1", multipleColoCurrentVersion);
    coloToVersionMap.put("colo-2", multipleColoCurrentVersion);
    StoreResponse storeResponse = getMockStoreResponse(coloToVersionMap, -1);
    when(controllerClient.getStore(STORE_NAME)).thenReturn(storeResponse);
    configureClusterDiscoveryControllerClient(controllerClient);

    final String userProvidedTopicName = Version.composeKafkaTopic(STORE_NAME_2, multipleColoCurrentVersion);
    Map<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(VENICE_STORE_NAME_PROP, STORE_NAME);
    overrideProperties.put(KAFKA_INPUT_TOPIC, userProvidedTopicName);
    try (VenicePushJob venicePushJob = new VenicePushJob(JOB_ID, getJobProperties(overrideProperties))) {
      venicePushJob.setControllerClient(controllerClient);
      venicePushJob.initKIFRepushDetails();
      Assert.assertEquals(venicePushJob.getPushJobSetting().kafkaInputTopic, userProvidedTopicName);
    }
  }

  private void configureClusterDiscoveryControllerClient(ControllerClient controllerClient) {
    D2ServiceDiscoveryResponse clusterDiscoveryResponse = mock(D2ServiceDiscoveryResponse.class);
    when(clusterDiscoveryResponse.isError()).thenReturn(false);
    when(clusterDiscoveryResponse.getCluster()).thenReturn("some-cluster");
    when(controllerClient.discoverCluster(STORE_NAME)).thenReturn(clusterDiscoveryResponse);
  }

  private StoreResponse getMockStoreResponse(Map<String, Integer> coloToVersionMap, int currentVersion) {
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(storeInfo.getColoToCurrentVersions()).thenReturn(coloToVersionMap);
    when(storeInfo.getCurrentVersion()).thenReturn(currentVersion);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    return storeResponse;
  }

  private StoreResponse getMockHybridStoreResponse(
      Map<String, Integer> coloToVersionMap,
      int currentVersion,
      BufferReplayPolicy bufferReplayPolicy) {
    StoreResponse storeResponse = mock(StoreResponse.class);
    HybridStoreConfig hybridStoreConfig = mock(HybridStoreConfig.class);
    StoreInfo storeInfo = mock(StoreInfo.class);
    when(hybridStoreConfig.getBufferReplayPolicy()).thenReturn(bufferReplayPolicy);
    when(storeInfo.getHybridStoreConfig()).thenReturn(hybridStoreConfig);
    when(storeInfo.getColoToCurrentVersions()).thenReturn(coloToVersionMap);
    when(storeInfo.getCurrentVersion()).thenReturn(currentVersion);
    when(storeResponse.getStore()).thenReturn(storeInfo);
    return storeResponse;
  }

  private RepushInfoResponse getMockRepushResponse(int versionNum) {
    RepushInfoResponse repushInfoResponse = mock(RepushInfoResponse.class);
    RepushInfo repushInfo = mock(RepushInfo.class);
    Version version = mock(Version.class);
    when(version.getNumber()).thenReturn(versionNum);
    when(repushInfo.getVersion()).thenReturn(version);
    when(repushInfo.getKafkaBrokerUrl()).thenReturn("kafkaUrl");
    when(repushInfoResponse.getRepushInfo()).thenReturn(repushInfo);
    return repushInfoResponse;
  }

  private Properties getJobProperties(Map<String, String> overrideConfigs) {
    Properties properties = new Properties();
    overrideConfigs.forEach(properties::setProperty);
    properties.setProperty(MULTI_REGION, "false");

    String childRegion = "child_region";
    properties.setProperty(SOURCE_GRID_FABRIC, childRegion);
    properties.setProperty(D2_ZK_HOSTS_PREFIX + childRegion, "child.zk.com:1234");

    properties.setProperty(SSL_KEY_PASSWORD_PROPERTY_NAME, "something");
    properties.setProperty(SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, "something");
    properties.setProperty(SSL_KEY_STORE_PROPERTY_NAME, "something");
    properties.setProperty(SSL_TRUST_STORE_PROPERTY_NAME, "something");
    properties.setProperty(SOURCE_KAFKA, "true");
    properties.setProperty(KAFKA_INPUT_BROKER_URL, "some-kafka-input-broker-url");
    properties.setProperty(VENICE_DISCOVER_URL_PROP, "some-venice-URL");
    return properties;
  }
}
