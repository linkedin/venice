package com.linkedin.venice.pushstatushelper;

import static com.linkedin.venice.common.PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX;
import static com.linkedin.venice.common.PushStatusStoreUtils.getServerIncrementalPushKey;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anySet;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEqualsDeep;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PushStatusStoreReaderTest {
  private static final String CLUSTER_DISCOVERY_D2_SERVICE_NAME =
      ClientConfig.DEFAULT_CLUSTER_DISCOVERY_D2_SERVICE_NAME + "_test";

  private D2Client d2ClientMock;
  private AvroSpecificStoreClient<PushStatusKey, PushStatusValue> storeClientMock;
  private final static int storeVersion = 42;
  private final static String storeName = "venice-test-push-status-store";
  private final static String incPushVersion = "ip-2022";
  private final static int partitionCount = 2;
  private final static int replicationFactor = 3;

  @BeforeMethod
  public void setUp() {
    d2ClientMock = mock(D2Client.class);
    storeClientMock = mock(AvroSpecificStoreClient.class);
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
      throws ExecutionException, InterruptedException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    // simulate store client returns null for given keys
    when(completableFutureMock.get()).thenReturn(Collections.emptyMap());

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
    System.out.println(result);
    for (Map<CharSequence, Integer> status: result.values()) {
      assertEqualsDeep(status, Collections.emptyMap());
    }
  }

  @Test(expectedExceptions = VeniceException.class, description = "Expect exception when result when push status read fails for some partitions")
  public void testGetPartitionStatusesWhenPushStatusReadFailsForSomePartitions()
      throws ExecutionException, InterruptedException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    // simulate store client returns null for given keys
    when(completableFutureMock.get()).thenReturn(null);

    Map<Integer, Map<CharSequence, Integer>> result =
        storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
    assertEqualsDeep(result, Collections.emptyMap());
  }

  @Test(expectedExceptions = VeniceException.class, description = "Expect an exception when push status store client throws an exception")
  public void testGetPartitionStatusesWhenStoreClientThrowsException() throws ExecutionException, InterruptedException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    // simulate store client returns an exception when fetching status info for given keys
    when(completableFutureMock.get()).thenThrow(new ExecutionException(new Throwable("Mock execution exception")));

    storeReaderSpy.getPartitionStatuses(storeName, storeVersion, incPushVersion, partitionCount);
  }

  @Test(description = "Expect statuses of all replicas when store returns all replica statuses")
  public void testGetPartitionStatusesWhenStoreReturnStatusesOfAllReplicas()
      throws ExecutionException, InterruptedException {
    Map<PushStatusKey, PushStatusValue> pushStatusMap =
        getPushStatusInstanceData(storeVersion, incPushVersion, partitionCount, replicationFactor);

    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<Map<PushStatusKey, PushStatusValue>> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.batchGet(pushStatusMap.keySet())).thenReturn(completableFutureMock);
    when(completableFutureMock.get()).thenReturn(pushStatusMap);

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
  public void testGetPartitionStatusesWhenStatusOfPartitionIsMissing() throws ExecutionException, InterruptedException {
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
    when(completableFutureMock.get()).thenReturn(pushStatusMap);

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
      throws ExecutionException, InterruptedException {
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
    when(completableFutureMock.get()).thenReturn(pushStatusMap);

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
      throws ExecutionException, InterruptedException {
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
      when(completableFutureMock.get()).thenReturn(statuses);
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
      throws ExecutionException, InterruptedException {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<PushStatusValue> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.get(pushStatusKey)).thenReturn(completableFutureMock);
    when(completableFutureMock.get()).thenReturn(null);

    assertEqualsDeep(
        storeReaderSpy.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion),
        Collections.emptyMap());
    verify(completableFutureMock).get();
    verify(storeClientMock).get(pushStatusKey);
  }

  @Test(description = "Expect an empty result when inc push versions are missing in the returned result")
  public void testGetSupposedlyOngoingIncrementalPushVersionsWhenIncPushVersionsAreMissing()
      throws ExecutionException, InterruptedException {
    PushStatusKey pushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    PushStatusValue pushStatusValue = new PushStatusValue();
    pushStatusValue.instances = null; // to make intentions clear explicitly setting it to null
    PushStatusStoreReader storeReaderSpy =
        spy(new PushStatusStoreReader(d2ClientMock, CLUSTER_DISCOVERY_D2_SERVICE_NAME, 10));
    CompletableFuture<PushStatusValue> completableFutureMock = mock(CompletableFuture.class);

    doReturn(storeClientMock).when(storeReaderSpy).getVeniceClient(any());
    when(storeClientMock.get(pushStatusKey)).thenReturn(completableFutureMock);
    when(completableFutureMock.get()).thenReturn(pushStatusValue);

    assertEqualsDeep(
        storeReaderSpy.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion),
        Collections.emptyMap());
    verify(completableFutureMock).get();
    verify(storeClientMock).get(pushStatusKey);
  }

  @Test(description = "Expect all inc push versions when inc push versions are found in push status store")
  public void testGetSupposedlyOngoingIncrementalPushVersionsWhenIncPushVersionsAreAvailable()
      throws ExecutionException, InterruptedException {
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
    when(completableFutureMock.get()).thenReturn(pushStatusValue);

    assertEqualsDeep(
        storeReaderSpy.getSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion),
        pushStatusValue.instances);
    verify(completableFutureMock).get();
    verify(storeClientMock).get(pushStatusKey);
  }
}
