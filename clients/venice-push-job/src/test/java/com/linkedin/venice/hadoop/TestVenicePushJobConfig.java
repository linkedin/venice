package com.linkedin.venice.hadoop;

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
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestPushUtils;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
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

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush with TTL is only supported while using Kafka Input Format.*")
  public void testRepushTTLJobWithNonKafkaInput() {
    Properties repushProps = new Properties();
    repushProps.put(VenicePushJob.REPUSH_TTL_IN_HOURS, 10L);
    VenicePushJob pushJob = getSpyVenicePushJob(Optional.of(repushProps), Optional.empty());
    pushJob.run();
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Repush TTL is not supported when the store has chunking enabled.*")
  public void testRepushTTLJobWithChunking() {
    Properties repushProps = new Properties();
    repushProps.put(VenicePushJob.REPUSH_TTL_IN_HOURS, 10L);
    repushProps.setProperty(VenicePushJob.SOURCE_KAFKA, "true");
    repushProps.setProperty(VenicePushJob.KAFKA_INPUT_TOPIC, Version.composeKafkaTopic(TEST_STORE, 0));
    repushProps.setProperty(VenicePushJob.KAFKA_INPUT_BROKER_URL, "localhost");
    repushProps.setProperty(VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "5");

    ControllerClient client = getClient(storeInfo -> {
      Version version = new VersionImpl(TEST_STORE, 0, TEST_PUSH);
      version.setChunkingEnabled(true);
      storeInfo.setChunkingEnabled(true);
      storeInfo.setVersions(Collections.singletonList(version));
    });
    VenicePushJob pushJob = getSpyVenicePushJob(Optional.of(repushProps), Optional.of(client));
    pushJob.run();
  }

  private VenicePushJob getSpyVenicePushJob() {
    return getSpyVenicePushJob(Optional.empty(), Optional.empty());
  }

  private VenicePushJob getSpyVenicePushJob(Optional<Properties> props, Optional<ControllerClient> client) {
    Properties baseProps = TestPushUtils.defaultVPJProps(TEST_URL, TEST_PATH, TEST_STORE);
    // for mocked tests, only attempt once.
    baseProps.put(VenicePushJob.CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1);
    props.ifPresent(baseProps::putAll);
    ControllerClient mockClient = client.orElseGet(this::getClient);
    VenicePushJob pushJob = spy(new VenicePushJob(TEST_PUSH, baseProps, mockClient, mockClient));
    pushJob.setSystemKMEStoreControllerClient(mockClient);
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
    storeInfo.setIncrementalPushEnabled(false);
    storeInfo.setStorageQuotaInByte(1L);
    storeInfo.setSchemaAutoRegisterFromPushJobEnabled(false);
    storeInfo.setChunkingEnabled(false);
    storeInfo.setCompressionStrategy(CompressionStrategy.NO_OP);
    storeInfo.setWriteComputationEnabled(false);
    storeInfo.setLeaderFollowerModelEnabled(false);
    storeInfo.setIncrementalPushEnabled(false);

    Version version = new VersionImpl(TEST_STORE, 0, TEST_PUSH);
    storeInfo.setVersions(Collections.singletonList(version));
    info.accept(storeInfo);
    return storeInfo;
  }
}
