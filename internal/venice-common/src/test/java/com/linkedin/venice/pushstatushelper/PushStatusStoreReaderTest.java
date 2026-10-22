package com.linkedin.venice.pushstatushelper;

import static com.linkedin.venice.common.PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX;
import static com.linkedin.venice.common.PushStatusStoreUtils.getServerIncrementalPushKey;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.PushMonitorUtils;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.utils.DataProviderUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PushStatusStoreReaderTest {
  private static final String CLUSTER_DISCOVERY_D2_SERVICE_NAME =
      ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME + "_test";

  private D2Client d2ClientMock;
  private AvroSpecificStoreClient<PushStatusKey, PushStatusValue> storeClientMock;
  private AvroSpecificStoreClient<PushStatusKey, PushStatusValue> clusterClient;
  private final static int storeVersion = 42;
  private final static String storeName = "venice-test-push-status-store";
  private final static String incPushVersion = "ip-2022";
  private final static int partitionCount = 2;
  private final static int replicationFactor = 3;

  @BeforeMethod
  public void setUp() {
    d2ClientMock = mock(D2Client.class);
    storeClientMock = mock(AvroSpecificStoreClient.class);
    clusterClient = mock(AvroSpecificStoreClient.class);
  }

  private Map<PushStatusKey, PushStatusValue> getPushStatusInstanceData(
      int version,
      String incrementalPushVersion,
      int numberOfPartitions,
      int replicationFactor) {
    Map<PushStatusKey, PushStatusValue> pushStatusMap = new HashMap<>();
    for (int i = 0; i < numberOfPartitions; i++) {
      PushStatusValue pushStatusValue = new PushStatusValue();
      pushStatusValue.instances = new HashMap<>();
      for (int j = 0; j < replicationFactor; j++) {
        pushStatusValue.instances.put("instance-" + j, END_OF_INCREMENTAL_PUSH_RECEIVED.getValue());
      }
      pushStatusMap.put(
          PushStatusStoreUtils
              .getServerIncrementalPushKey(version, i, incrementalPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX),
          pushStatusValue);
    }
    return pushStatusMap;
  }

  @Test(description = "Expect empty results when push status info is not available for any of the partition")
  public void testGetPartitionStatusesWhenPushStatusesAreNotAvailable()
      throws ExecutionException, InterruptedException, TimeoutException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    // simulate store client returns null for given keys
    when(completableFutureMock.get(anyLong(), any())).thenReturn(Collections.emptyMap());

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
    System.out.println(result);
    for (Map<CharSequence, Integer> status: result.values()) {
      assertEqualsDeep(status, Collections.emptyMap());
    }
  }

  @Test(expectedExceptions = VeniceException.class, description = "Expect exception when result when push status read fails for some partitions")
  public void testGetPartitionStatusesWhenPushStatusReadFailsForSomePartitions()
      throws ExecutionException, InterruptedException, TimeoutException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    // simulate store client returns null for given keys
    when(completableFutureMock.get(anyLong(), any())).thenReturn(null);

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
    assertEqualsDeep(result, Collections.emptyMap());
  }

  @Test(expectedExceptions = VeniceException.class, description = "Expect an exception when push status store client throws an exception")
  public void testGetPartitionStatusesWhenStoreClientThrowsException()
      throws ExecutionException, InterruptedException, TimeoutException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    // simulate store client returns an exception when fetching status info for given keys
    when(completableFutureMock.get(anyLong(), any()))
        .thenThrow(new ExecutionException(new Throwable("Mock execution exception")));

    storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
  }

  @Test(description = "Expect statuses of all replicas when store returns all replica statuses")
  public void testGetPartitionStatusesWhenStoreReturnStatusesOfAllReplicas()
      throws ExecutionException, InterruptedException, TimeoutException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    when(completableFutureMock.get(anyLong(), any())).thenReturn(pushStatusMap);

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
    assertNotEquals(result.size(), 0);
    for (Map.Entry<PushStatusKey, PushStatusValue> pushStatus: pushStatusMap.entrySet()) {
      assertEqualsDeep(
          result.get(PushStatusStoreUtils.getPartitionIdFromServerIncrementalPushKey(pushStatus.getKey())),
          pushStatus.getValue().instances);
    }
  }

  @Test(description = "Expect empty status when statuses for replicas of a partition is missing")
  public void testGetPartitionStatusesWhenStatusOfPartitionIsMissing()
      throws ExecutionException, InterruptedException, TimeoutException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);
    // erase status of partitionId 0
    pushStatusMap.put(
        PushStatusStoreUtils
            .getServerIncrementalPushKey(storeVersion, 0, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX),
        null);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    when(completableFutureMock.get(anyLong(), any())).thenReturn(pushStatusMap);

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
    assertNotEquals(result.size(), 0);
    // for partitionId 0 expect empty status
    assertEqualsDeep(result.get(0), Collections.emptyMap());
    // for partitionId 1 expect status of its replicas
    assertEqualsDeep(
        result.get(1),
        pushStatusMap.get(
            getServerIncrementalPushKey(storeVersion, 1, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX)).instances);
  }

  @Test(description = "Expect empty status when instance info for replicas of a partition is missing")
  public void testGetPartitionStatusesWhenInstanceInfoOfPartitionIsMissing()
      throws ExecutionException, InterruptedException, TimeoutException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);
    // set empty status for partitionId 0
    pushStatusMap.put(
        PushStatusStoreUtils
            .getServerIncrementalPushKey(storeVersion, 0, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX),
        new PushStatusValue());

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    when(completableFutureMock.get(anyLong(), any())).thenReturn(pushStatusMap);

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);

    assertNotEquals(result.size(), 0);
    // for partitionId 0 expect empty status
    assertEqualsDeep(result.get(0), Collections.emptyMap());
    // for partitionId 1 expect status of its replicas
    assertEqualsDeep(
        result.get(1),
        pushStatusMap.get(
            getServerIncrementalPushKey(storeVersion, 1, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX)).instances);
  }

  @Test(description = "Expect all statuses even when number of partitions are greater than the batchGetLimit")
  public void testGetPartitionStatusesWhenNumberOfPartitionsAreGreaterThanBatchGetLimit()
      throws ExecutionException, InterruptedException, TimeoutException {
    int partitionCount = 1055;
    int batchGetLimit = 256;

    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, 1);
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());

    List<Set<PushStatusKey>> keySets = new ArrayList<>();
    for (int partition = 0; partition < partitionCount; partition += batchGetLimit) {
      Map<PushStatusKey, PushStatusValue> statuses = new HashMap<>();
      for (int i = partition; i < (partition + batchGetLimit) && i < partitionCount; i++) {
        PushStatusKey key = PushStatusStoreUtils.getServerIncrementalPushKey(
            storeVersion,
            i,
            incPushVersion,
            PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX);
        statuses.put(key, pushStatusMap.get(key));
      }
      keySets.add(statuses.keySet());
      CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);
      when(storeClientMock.batchGet(eq(statuses.keySet()))).thenReturn(completableFutureMock);
      when(completableFutureMock.get(anyLong(), any())).thenReturn(statuses);
    }

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount, batchGetLimit);
    assertNotEquals(result.size(), 0);
    for (Map.Entry<PushStatusKey, PushStatusValue> pushStatus: pushStatusMap.entrySet()) {
      assertEqualsDeep(
          result.get(PushStatusStoreUtils.getPartitionIdFromServerIncrementalPushKey(pushStatus.getKey())),
          pushStatus.getValue().instances);
    }

    // 1055 keys means 4 full batches of 256 keys and 1 batch of 31 keys
    verify(storeClientMock, times(5)).batchGet(anySet());
    for (Set<PushStatusKey> keySet: keySets) {
      verify(storeClientMock).batchGet(keySet);
    }
  }

  @Test(description = "Expect an exception if venice system store client throws an exception", expectedExceptions = VeniceException.class)
  public void testGetSupposedlyOngoingIncrementalPushVersionsWithClientException() {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.get(pushStatusKey)).thenThrow(VeniceClientException.class);

    storeReaderSpy.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion);
    verify(storeClientMock).get(pushStatusKey);
    verify(storeReaderSpy).getVeniceClient(any());
  }

  @Test(description = "Expect an empty result when key-value for ongoing incremental pushes doesn't exist")
  public void testGetSupposedlyOngoingIncrementalPushVersionsWhenIncPushVersionsDoesNotExist()
      throws ExecutionException, InterruptedException, TimeoutException {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<PushStatusValue> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.get(pushStatusKey)).thenReturn(completableFutureMock);
    when(completableFutureMock.get(anyLong(), any())).thenReturn(null);

    assertEqualsDeep(
        storeReaderSpy.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion),
        Collections.emptyMap());
    verify(completableFutureMock).get(60, TimeUnit.SECONDS);
    verify(storeClientMock).get(pushStatusKey);
  }

  @Test(description = "Expect an empty result when inc push versions are missing in the returned result")
  public void testGetSupposedlyOngoingIncrementalPushVersionsWhenIncPushVersionsAreMissing()
      throws ExecutionException, InterruptedException, TimeoutException {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusValue pushStatusValue = new PushStatusValue();
    pushStatusValue.instances = null; // to make intentions clear explicitly setting it to null
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<PushStatusValue> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.get(pushStatusKey)).thenReturn(completableFutureMock);
    when(completableFutureMock.get(anyLong(), any())).thenReturn(pushStatusValue);

    assertEqualsDeep(
        storeReaderSpy.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion),
        Collections.emptyMap());
    verify(completableFutureMock).get(60, TimeUnit.SECONDS);
    verify(storeClientMock).get(pushStatusKey);
  }

  @Test(description = "Expect all inc push versions when inc push versions are found in push status store")
  public void testGetSupposedlyOngoingIncrementalPushVersionsWhenIncPushVersionsAreAvailable()
      throws ExecutionException, InterruptedException, TimeoutException {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusValue pushStatusValue = new PushStatusValue();
    pushStatusValue.instances = new HashMap<>();
    pushStatusValue.instances.put("inc_push_v1", 7);
    pushStatusValue.instances.put("inc_push_v2", 7);
    pushStatusValue.instances.put("inc_push_v3", 7);
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<PushStatusValue> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.get(pushStatusKey)).thenReturn(completableFutureMock);
    when(completableFutureMock.get(anyLong(), any())).thenReturn(pushStatusValue);

    assertEqualsDeep(
        storeReaderSpy.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion),
        pushStatusValue.instances);
    verify(completableFutureMock).get(60, TimeUnit.SECONDS);
    verify(storeClientMock).get(pushStatusKey);
  }

  @Test
  public void testNullResponseWhenVersionLevelKeyIsNotWritten()
      throws ExecutionException, InterruptedException, TimeoutException {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getPushKey(storeVersion);
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    CompletableFuture<PushStatusValue> completableFutureMock = mock(CompletableFuture.class);
    when(storeClientMock.get(pushStatusKey)).thenReturn(completableFutureMock);
    // simulate store client returns null for given keys
    when(completableFutureMock.get(anyLong(), any())).thenReturn(null);

    // Test that push status store reader will also return null instead of empty map in this case
    Assert.assertNull(storeReaderSpy.getVersionStatus(storeName, storeVersion, Optional.empty()));
  }

  @Test
  public void testGetInstanceStatus() {
    PushStatusStoreReader mockReader = mock(PushStatusStoreReader.class);
    doCallRealMethod().when(mockReader).getInstanceStatus(any(), any());

    doReturn(-1l).when(mockReader).getHeartbeat("store_1", "instance_1");
    assertEquals(
        mockReader.getInstanceStatus("store_1", "instance_1"),
        PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING);

    doReturn(1000l).when(mockReader).getHeartbeat("store_1", "instance_1");
    doReturn(true).when(mockReader).isInstanceAlive(anyLong());
    assertEquals(mockReader.getInstanceStatus("store_1", "instance_1"), PushStatusStoreReader.InstanceStatus.ALIVE);

    doReturn(false).when(mockReader).isInstanceAlive(anyLong());
    assertEquals(mockReader.getInstanceStatus("store_1", "instance_1"), PushStatusStoreReader.InstanceStatus.DEAD);
  }

  @Test
  public void testGetPartitionStatusAsyncSuccess() throws Exception {
    // Setup
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(anyString());

    String storeName = "testStore";
    int version = 1;
    int partitionId = 2;
    Optional<String> incrementalPushVersion = Optional.empty();
    Optional<String> incrementalPushPrefix = Optional.empty();

    // Create the expected result map
    Map<CharSequence, Integer> expectedMap = new HashMap<>();
    expectedMap.put("instance1", 1);
    expectedMap.put("instance2", 2);

    PushStatusValue mockStatusValue = mock(PushStatusValue.class);
    mockStatusValue.instances = expectedMap;

    when(storeClientMock.get(any(PushStatusKey.class))).thenReturn(CompletableFuture.completedFuture(mockStatusValue));

    // Execute the method
    CompletableFuture<Map<CharSequence, Integer>> future = storeReaderSpy.getPartitionOrVersionStatusAsync(
        storeName,
        version,
        partitionId,
        incrementalPushVersion,
        incrementalPushPrefix,
        false);

    Map<CharSequence, Integer> result = future.get(5, TimeUnit.SECONDS);

    // Verify the result
    Assert.assertEquals(expectedMap, result);
    verify(storeClientMock).get(any(PushStatusKey.class));
  }

  @Test
  public void testGetPartitionStatusAsyncExceptionHandling() throws Exception {
    // Setup
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(anyString());

    String storeName = "testStore";
    int version = 1;
    int partitionId = 2;
    Optional incrementalPushVersion = Optional.empty();
    Optional incrementalPushPrefix = Optional.empty();

    // Mock an exception during async call
    when(storeClientMock.get(any(PushStatusKey.class))).thenReturn(CompletableFuture.supplyAsync(() -> {
      throw new RuntimeException("Simulated exception");
    }));

    // Execute the method
    CompletableFuture<Map<String, Integer>> future = storeReaderSpy.getPartitionOrVersionStatusAsync(
        storeName,
        version,
        partitionId,
        incrementalPushVersion,
        incrementalPushPrefix,
        false);

    try {
      future.get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertTrue(e.getCause().getMessage().contains("Simulated exception"));
      verify(storeClientMock).get(any(PushStatusKey.class));
    }
  }

  @Test
  public void testClientConfig() {
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));

    ClientConfig clientConfig = storeReaderSpy.getClientConfig(storeName);
    Assert.assertFalse(clientConfig.isStatTrackingEnabled());
  }

  private PushStatusStoreReader clusterReader() {
    return clusterReader(store -> "cluster");
  }

  private PushStatusStoreReader clusterReader(Function<String, String> resolver) {
    PushStatusStoreReader reader =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10, resolver));
    doReturn(storeClientMock).when(reader).getVeniceClient(anyString());
    doReturn(clusterClient).when(reader).getClusterVeniceClient("cluster");
    return reader;
  }

  private PushStatusValue status(CharSequence instance, int status) {
    PushStatusValue value = new PushStatusValue();
    value.instances = Collections.singletonMap(instance, status);
    return value;
  }

  @DataProvider
  public Object[][] legacyOverlapStatuses() {
    return new Object[][] { { ExecutionStatus.COMPLETED }, { ExecutionStatus.ERROR },
        { ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED }, { ExecutionStatus.STARTED } };
  }

  @Test(dataProvider = "legacyOverlapStatuses")
  public void testClusterReadsMergeMixedClientsAcrossAllReadPaths(ExecutionStatus legacyStatus) throws Exception {
    try (PushStatusStoreReader reader = clusterReader()) {
      PushStatusValue legacy = status(new Utf8("legacy"), legacyStatus.getValue());
      PushStatusValue primary = status("new", ExecutionStatus.COMPLETED.getValue());
      primary.instances = new HashMap<>(primary.instances);
      primary.instances.put("legacy", ExecutionStatus.STARTED.getValue());
      Map<CharSequence, Integer> expected = new HashMap<>();
      expected.put("legacy", legacyStatus.getValue());
      expected.put("new", ExecutionStatus.COMPLETED.getValue());
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(legacy));
      when(clusterClient.get(any())).thenReturn(CompletableFuture.completedFuture(primary));

      assertEquals(reader.getVersionStatus(storeName, storeVersion, Optional.empty()), expected);
      assertEquals(reader.getPartitionStatus(storeName, storeVersion, 0, Optional.empty()), expected);
      assertEquals(
          reader.getPartitionOrVersionStatusAsync(storeName, storeVersion, 0, Optional.empty(), Optional.empty(), true)
              .get(),
          expected);
      assertEquals(reader.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion), expected);
      verify(clusterClient).get(
          PushStatusStoreUtils.getClusterPushStatusKey(
              storeName,
              PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion)));

      PushStatusKey partitionKey =
          getServerIncrementalPushKey(storeVersion, 0, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX);
      when(storeClientMock.batchGet(Collections.singleton(partitionKey)))
          .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(partitionKey, legacy)));
      PushStatusKey clusterKey = PushStatusStoreUtils.getClusterPushStatusKey(storeName, partitionKey);
      when(clusterClient.batchGet(Collections.singleton(clusterKey)))
          .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(clusterKey, primary)));
      assertEquals(reader.getPartitionStatuses(storeName, storeVersion, incPushVersion, 1).get(0), expected);
      doReturn(PushStatusStoreReader.InstanceStatus.ALIVE).when(reader).getInstanceStatus(anyString(), anyString());
      assertEquals(
          PushMonitorUtils
              .getDaVinciPushStatusAndDetails(reader, storeName + "_v" + storeVersion, 1, Optional.empty(), 0, 0, false)
              .getStatus(),
          legacyStatus == END_OF_INCREMENTAL_PUSH_RECEIVED ? ExecutionStatus.STARTED : legacyStatus);

      primary.instances = Collections.singletonMap("legacy", ExecutionStatus.COMPLETED.getValue());
      assertEquals(
          reader.getVersionStatus(storeName, storeVersion, Optional.empty()),
          Collections.singletonMap("legacy", legacyStatus.getValue()));
      Assert.assertTrue(legacy.instances.keySet().iterator().next() instanceof Utf8);
    }
  }

  @Test
  public void testClusterReadFallbackAndMissingRecordSemantics() {
    try (PushStatusStoreReader reader = clusterReader()) {
      PushStatusValue legacy = status(new Utf8("legacy"), 1);
      Map<CharSequence, Integer> expected = Collections.singletonMap("legacy", 1);
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(legacy));
      when(clusterClient.get(any())).thenReturn(CompletableFuture.completedFuture(null));
      assertEquals(reader.getVersionStatus(storeName, storeVersion, Optional.empty()), expected);
      CompletableFuture<PushStatusValue> failed = new CompletableFuture<>();
      failed.completeExceptionally(new VeniceClientException("cluster unavailable"));
      when(clusterClient.get(any())).thenReturn(failed);
      assertEquals(reader.getVersionStatus(storeName, storeVersion, Optional.empty()), expected);
      when(clusterClient.get(any())).thenThrow(new VeniceClientException("cluster unavailable"));
      assertEquals(reader.getVersionStatus(storeName, storeVersion, Optional.empty()), expected);
      doReturn(new CompletableFuture<PushStatusValue>()).when(clusterClient).get(any());
      assertEquals(reader.getVersionStatus(storeName, storeVersion, Optional.empty()), expected);

      when(clusterClient.get(any())).thenReturn(CompletableFuture.completedFuture(null));
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(null));
      Assert.assertNull(reader.getVersionStatus(storeName, storeVersion, Optional.empty()));
      Assert.assertTrue(reader.getPartitionStatus(storeName, storeVersion, 0, Optional.empty()).isEmpty());
      PushStatusValue primary = status(new Utf8("new"), 2);
      when(clusterClient.get(any())).thenReturn(CompletableFuture.completedFuture(primary));
      assertEquals(
          reader.getVersionStatus(storeName, storeVersion, Optional.empty()),
          Collections.singletonMap("new", 2));
      primary.instances = null;
      Assert.assertNull(reader.getVersionStatus(storeName, storeVersion, Optional.empty()));
      primary.instances = Collections.emptyMap();
      Assert.assertTrue(reader.getVersionStatus(storeName, storeVersion, Optional.empty()).isEmpty());
      legacy.instances = null;
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(legacy));
      Assert.assertTrue(reader.getVersionStatus(storeName, storeVersion, Optional.empty()).isEmpty());
      primary.instances = null;
      Assert.assertTrue(reader.getVersionStatus(storeName, storeVersion, Optional.empty()).isEmpty());
      when(clusterClient.get(any())).thenReturn(CompletableFuture.completedFuture(null));
      Assert.assertNull(reader.getVersionStatus(storeName, storeVersion, Optional.empty()));
      when(storeClientMock.get(any())).thenReturn(failed);
      Assert.assertThrows(
          VeniceException.class,
          () -> reader.getVersionStatus(storeName, storeVersion, Optional.empty()));
    }
  }

  @Test
  public void testLegacyHeartbeatIsAuthoritativeAcrossBootstrapRestarts() {
    try (PushStatusStoreReader reader = clusterReader()) {
      PushStatusValue legacy = new PushStatusValue();
      PushStatusValue primary = new PushStatusValue();
      legacy.reportTimestamp = 1L;
      primary.reportTimestamp = 2L;
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(legacy));
      when(clusterClient.get(any())).thenReturn(new CompletableFuture<>());
      assertEquals(reader.getHeartbeat(storeName, "instance"), 1L);
      legacy.reportTimestamp = -1L;
      assertEquals(reader.getInstanceStatus(storeName, "instance"), PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING);
      legacy.reportTimestamp = System.currentTimeMillis();
      primary.reportTimestamp = -1L;
      assertEquals(reader.getInstanceStatus(storeName, "instance"), PushStatusStoreReader.InstanceStatus.ALIVE);
      legacy.reportTimestamp = -1L;
      primary.reportTimestamp = System.currentTimeMillis();
      assertEquals(reader.getInstanceStatus(storeName, "instance"), PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING);
      verifyNoInteractions(clusterClient);
      when(clusterClient.get(any())).thenReturn(CompletableFuture.completedFuture(primary));
      legacy.reportTimestamp = null;
      assertEquals(reader.getHeartbeat(storeName, "instance"), primary.reportTimestamp.longValue());
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(null));
      primary.reportTimestamp = -1L;
      assertEquals(reader.getInstanceStatus(storeName, "instance"), PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING);
    }
  }

  @Test
  public void testMissingHeartbeatUsesBoundedClusterFallbackAndPropagatesLegacyFailure() {
    try (PushStatusStoreReader reader = clusterReader()) {
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(null));
      when(clusterClient.get(any())).thenReturn(new CompletableFuture<>());
      assertEquals(reader.getHeartbeat(storeName, "instance"), 0L);
      CompletableFuture<PushStatusValue> failure = new CompletableFuture<>();
      failure.completeExceptionally(new VeniceClientException("heartbeat unavailable"));
      when(clusterClient.get(any())).thenReturn(failure);
      assertEquals(reader.getHeartbeat(storeName, "instance"), 0L);
      when(clusterClient.get(any())).thenReturn(CompletableFuture.completedFuture(null));
      assertEquals(reader.getHeartbeat(storeName, "instance"), 0L);
      when(storeClientMock.get(any())).thenReturn(failure);
      Assert.assertThrows(VeniceException.class, () -> reader.getHeartbeat(storeName, "instance"));
    }
  }

  @Test(dataProvider = "legacyOverlapStatuses")
  public void testMergedVersionStatusExcludesLegacyOnlyPartitionReport(ExecutionStatus obsoleteStatus) {
    try (PushStatusStoreReader reader = clusterReader()) {
      PushStatusKey versionKey = PushStatusStoreUtils.getPushKey(storeVersion, Optional.empty());
      PushStatusKey partitionKey = PushStatusStoreUtils.getPushKey(storeVersion, 0, Optional.empty(), Optional.empty());
      PushStatusValue legacyVersion = status(new Utf8("legacy"), ExecutionStatus.COMPLETED.getValue());
      PushStatusValue clusterVersion = status(new Utf8("cluster-only"), ExecutionStatus.COMPLETED.getValue());
      clusterVersion.instances = new HashMap<>(clusterVersion.instances);
      clusterVersion.instances.put("legacy", ExecutionStatus.STARTED.getValue());
      PushStatusValue heartbeat = new PushStatusValue();
      heartbeat.reportTimestamp = System.currentTimeMillis();
      Map<PushStatusKey, PushStatusValue> legacy = new HashMap<>();
      legacy.put(versionKey, legacyVersion);
      legacy.put(partitionKey, status(new Utf8("legacy"), obsoleteStatus.getValue()));
      legacy.put(PushStatusStoreUtils.getHeartbeatKey("legacy"), heartbeat);
      legacy.put(PushStatusStoreUtils.getHeartbeatKey("cluster-only"), heartbeat);
      when(storeClientMock.get(any()))
          .thenAnswer(invocation -> CompletableFuture.completedFuture(legacy.get(invocation.getArgument(0))));
      when(clusterClient.get(any())).thenAnswer(
          invocation -> CompletableFuture.completedFuture(
              PushStatusStoreUtils.getClusterPushStatusKey(storeName, versionKey).equals(invocation.getArgument(0))
                  ? clusterVersion
                  : null));

      assertEquals(
          PushMonitorUtils
              .getDaVinciPushStatusAndDetails(reader, storeName + "_v" + storeVersion, 1, Optional.empty(), 0, 0, false)
              .getStatus(),
          ExecutionStatus.COMPLETED);
    }
  }

  @Test
  public void testClusterBatchFallbackAndLegacyFailurePropagation() {
    try (PushStatusStoreReader reader = clusterReader()) {
      PushStatusKey key = getServerIncrementalPushKey(storeVersion, 0, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX);
      PushStatusValue legacy = status("legacy", 1);
      when(storeClientMock.batchGet(anySet()))
          .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(key, legacy)));
      CompletableFuture<Map<PushStatusKey, PushStatusValue>> failure = new CompletableFuture<>();
      failure.completeExceptionally(new VeniceClientException("cluster batch unavailable"));
      when(clusterClient.batchGet(anySet())).thenReturn(failure);
      assertEquals(reader.getPartitionStatuses(storeName, storeVersion, incPushVersion, 1).get(0), legacy.instances);
      when(clusterClient.batchGet(anySet())).thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));
      assertEquals(reader.getPartitionStatuses(storeName, storeVersion, incPushVersion, 1).get(0), legacy.instances);
      when(storeClientMock.batchGet(anySet())).thenReturn(failure);
      VeniceException expected = Assert.expectThrows(
          VeniceException.class,
          () -> reader.getPartitionStatuses(storeName, storeVersion, incPushVersion, 1));
      Assert.assertTrue(expected.getMessage().contains("Failed to fetch push statuses"));
    }
  }

  @Test
  public void testBlockedClusterWorkersRejectRepeatedReadsWithoutQueueing() throws Exception {
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    try (PushStatusStoreReader reader = clusterReader()) {
      PushStatusValue legacy = status("legacy", 1);
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(legacy));
      doAnswer(invocation -> {
        started.countDown();
        release.await();
        return storeClientMock;
      }).when(reader).getClusterVeniceClient("cluster");
      CompletableFuture<Map<CharSequence, Integer>> result =
          reader.getPartitionStatusAsync(storeName, storeVersion, 0, Optional.empty(), Optional.empty());
      CompletableFuture<Map<CharSequence, Integer>> secondResult =
          reader.getPartitionStatusAsync(storeName, storeVersion, 1, Optional.empty(), Optional.empty());
      try {
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < 50; i++) {
          CompletableFuture<Map<CharSequence, Integer>> rejected =
              reader.getPartitionStatusAsync(storeName, storeVersion, i, Optional.empty(), Optional.empty());
          Assert.assertTrue(rejected.isDone(), "Saturated cluster workers must fall back without queueing");
          assertEquals(rejected.get(), legacy.instances);
        }
        assertEquals(result.get(5, TimeUnit.SECONDS), legacy.instances);
        assertEquals(secondResult.get(5, TimeUnit.SECONDS), legacy.instances);
        verify(reader, times(2)).getClusterVeniceClient("cluster");
      } finally {
        release.countDown();
      }
    }
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testClusterResolverFailureCannotDiscardLegacyResponse(boolean batch, boolean blocked) throws Exception {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (PushStatusStoreReader reader = clusterReader(store -> {
      started.countDown();
      if (blocked) {
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new VeniceException(e);
        }
      }
      throw new IllegalStateException("cluster discovery unavailable");
    })) {
      PushStatusValue legacy = status("legacy", 1);
      PushStatusKey key = getServerIncrementalPushKey(storeVersion, 0, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX);
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(legacy));
      when(storeClientMock.batchGet(anySet()))
          .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(key, legacy)));
      CompletableFuture<Map<CharSequence, Integer>> result = CompletableFuture.supplyAsync(
          () -> batch
              ? reader.getPartitionStatuses(storeName, storeVersion, incPushVersion, 1).get(0)
              : reader.getVersionStatus(storeName, storeVersion, Optional.empty()));
      try {
        Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
        assertEquals(result.get(5, TimeUnit.SECONDS), legacy.instances);
        verifyNoInteractions(clusterClient);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void testDisabledClusterReadsKeepImmediateLegacyPath() {
    try (PushStatusStoreReader reader = clusterReader(null)) {
      PushStatusValue legacy = status("legacy", 1);
      legacy.reportTimestamp = -1L;
      PushStatusKey key = getServerIncrementalPushKey(storeVersion, 0, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX);
      when(storeClientMock.get(any())).thenReturn(CompletableFuture.completedFuture(legacy));
      when(storeClientMock.batchGet(anySet()))
          .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(key, legacy)));
      CompletableFuture<Map<CharSequence, Integer>> result =
          reader.getPartitionStatusAsync(storeName, storeVersion, 0, Optional.empty(), Optional.empty());
      Assert.assertTrue(result.isDone());
      assertEquals(result.join(), legacy.instances);
      assertEquals(reader.getPartitionStatuses(storeName, storeVersion, incPushVersion, 1).get(0), legacy.instances);
      assertEquals(reader.getHeartbeat(storeName, "instance"), -1L);
      verifyNoInteractions(clusterClient);
    }
  }
}
