package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestWriteUtils;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Compared to {@link TestVenicePushJob} which is the integration test for PushJob,
 * this unit test focus on configuration-related tests.
 */
public class TestVenicePushJobConfig {
  private static final String TEST_PUSH = "test_push";
  private static final String TEST_URL = "test_url";
  private static final String TEST_PATH = "test_path";
  private static final String TEST_STORE = "test_store";
  private static final String TEST_CLUSTER = "test_cluster";
  private static final String TEST_SERVICE = "test_venice";
  private static final int REPUSH_VERSION = 1;

  private static final String TEST_PARENT_ZK_ADDRESS = "localhost:2180";
  private static final String TEST_ZK_ADDRESS = "localhost:2181";
  private static final String TEST_PARENT_CONTROLLER_D2_SERVICE = "ParentController";
  private static final String TEST_CHILD_CONTROLLER_D2_SERVICE = "ChildController";

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush with TTL is only supported while using Kafka Input Format.*")
  public void testRepushTTLJobWithNonKafkaInput() {
    Properties repushProps = new Properties();
    repushProps.setProperty(REPUSH_TTL_ENABLE, "true");
    VenicePushJob pushJob = getSpyVenicePushJob(repushProps, null);
    pushJob.run();
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush TTL is only supported for real-time only store.*")
  public void testRepushTTLJobWithBatchStore() {
    Properties repushProps = getRepushReadyProps();

    ControllerClient client = getClient();
    VenicePushJob pushJob = getSpyVenicePushJob(repushProps, client);
    pushJob.run();
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush TTL is not supported when the store has write compute enabled.*")
  public void testRepushTTLJobWithWC() {
    Properties repushProps = getRepushReadyProps();

    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null, null));
    });
    VenicePushJob pushJob = getSpyVenicePushJob(repushProps, client);
    pushJob.run();
  }

  @Test
  public void testPushJobSettingWithD2Routing() {
    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null, null));
    });
    VenicePushJob pushJob = getSpyVenicePushJobWithD2Routing(new Properties(), client);
    VenicePushJob.PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
    Assert.assertTrue(pushJobSetting.d2Routing);
    Assert.assertEquals(pushJobSetting.controllerD2ServiceName, TEST_CHILD_CONTROLLER_D2_SERVICE);
    Assert.assertEquals(pushJobSetting.childControllerRegionD2ZkHosts, TEST_ZK_ADDRESS);

    VenicePushJob multiRegionPushJob = getSpyVenicePushJobWithMultiRegionD2Routing(new Properties(), client);
    VenicePushJob.PushJobSetting multiRegionPushJobSetting = multiRegionPushJob.getPushJobSetting();
    Assert.assertTrue(multiRegionPushJobSetting.d2Routing);
    Assert.assertEquals(multiRegionPushJobSetting.controllerD2ServiceName, TEST_PARENT_CONTROLLER_D2_SERVICE);
    Assert.assertEquals(multiRegionPushJobSetting.parentControllerRegionD2ZkHosts, TEST_PARENT_ZK_ADDRESS);
  }

  @Test
  public void testPushJobSettingWithLivenessHeartbeat() {
    Properties vpjProps = new Properties();
    vpjProps.setProperty(HEARTBEAT_ENABLED_CONFIG.getConfigName(), "true");
    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
      storeInfo.setWriteComputationEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
      storeInfo.setHybridStoreConfig(new HybridStoreConfigImpl(0, 0, 0, null, null));
    });
    VenicePushJob pushJob = getSpyVenicePushJob(vpjProps, client);
    VenicePushJob.PushJobSetting pushJobSetting = pushJob.getPushJobSetting();
    Assert.assertTrue(pushJobSetting.livenessHeartbeatEnabled);
  }

  private Properties getRepushReadyProps() {
    Properties repushProps = new Properties();
    repushProps.setProperty(REPUSH_TTL_ENABLE, "true");
    repushProps.setProperty(VenicePushJob.SOURCE_KAFKA, "true");
    repushProps.setProperty(VenicePushJob.KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(TEST_STORE, REPUSH_VERSION));
    repushProps.setProperty(VenicePushJob.KAFKA_INPUT_BROKER_URL, "localhost");
    repushProps.setProperty(VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");
    return repushProps;
  }

  private VenicePushJob getSpyVenicePushJob(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJProps(TEST_URL, TEST_PATH, TEST_STORE);
    return getSpyVenicePushJobInternal(baseProps, props, client);
  }

  private VenicePushJob getSpyVenicePushJobWithD2Routing(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJPropsWithD2Routing(
        null,
        null,
        Collections.singletonMap("dc-0", TEST_ZK_ADDRESS),
        null,
        TEST_CHILD_CONTROLLER_D2_SERVICE,
        TEST_PATH,
        TEST_STORE);
    return getSpyVenicePushJobInternal(baseProps, props, client);
  }

  private VenicePushJob getSpyVenicePushJobWithMultiRegionD2Routing(Properties props, ControllerClient client) {
    Properties baseProps = TestWriteUtils.defaultVPJPropsWithD2Routing(
        "dc-parent",
        TEST_PARENT_ZK_ADDRESS,
        Collections.singletonMap("dc-0", TEST_ZK_ADDRESS),
        TEST_PARENT_CONTROLLER_D2_SERVICE,
        TEST_CHILD_CONTROLLER_D2_SERVICE,
        TEST_PATH,
        TEST_STORE);
    return getSpyVenicePushJobInternal(baseProps, props, client);
  }

  private VenicePushJob getSpyVenicePushJobInternal(
      Properties baseVPJProps,
      Properties props,
      ControllerClient client) {
    // for mocked tests, only attempt once.
    baseVPJProps.put(VenicePushJob.CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1);
    baseVPJProps.putAll(props);
    ControllerClient mockClient = client == null ? getClient() : client;
    VenicePushJob pushJob = spy(new VenicePushJob(TEST_PUSH, baseVPJProps));
    pushJob.setControllerClient(mockClient);
    pushJob.setKmeSchemaSystemStoreControllerClient(mockClient);
    return pushJob;
  }

  private ControllerClient getClient() {
    return getClient(storeInfo -> {});
  }

  private ControllerClient getClient(Consumer<StoreInfo> storeInfo) {
    ControllerClient client = mock(ControllerClient.class);
    // mock discover cluster
    D2ServiceDiscoveryResponse clusterResponse = new D2ServiceDiscoveryResponse();
    clusterResponse.setCluster(TEST_CLUSTER);
    clusterResponse.setD2Service(TEST_SERVICE);
    doReturn(clusterResponse).when(client).discoverCluster(TEST_STORE);

    // mock value schema
    SchemaResponse schemaResponse = new SchemaResponse();
    doReturn(schemaResponse).when(client).getValueSchema(anyString(), anyInt());

    // mock storeinfo response
    StoreResponse storeResponse = new StoreResponse();
    storeResponse.setStore(getStoreInfo(storeInfo));
    doReturn(storeResponse).when(client).getStore(TEST_STORE);

    return client;
  }

  private StoreInfo getStoreInfo(Consumer<StoreInfo> info) {
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setCurrentVersion(REPUSH_VERSION);
    storeInfo.setIncrementalPushEnabled(false);
    storeInfo.setStorageQuotaInByte(1L);
    storeInfo.setSchemaAutoRegisterFromPushJobEnabled(false);
    storeInfo.setChunkingEnabled(false);
    storeInfo.setCompressionStrategy(CompressionStrategy.NO_OP);
    storeInfo.setWriteComputationEnabled(false);
    storeInfo.setLeaderFollowerModelEnabled(false);
    storeInfo.setIncrementalPushEnabled(false);

    Version version = new VersionImpl(TEST_STORE, REPUSH_VERSION, TEST_PUSH);
    storeInfo.setVersions(Collections.singletonList(version));
    info.accept(storeInfo);
    return storeInfo;
  }
}
