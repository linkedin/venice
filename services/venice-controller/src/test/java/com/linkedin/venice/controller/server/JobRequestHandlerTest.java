package com.linkedin.venice.controller.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.meta.UncompletedPartition;
import com.linkedin.venice.meta.UncompletedReplica;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcResponse;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class JobRequestHandlerTest {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_STORE = "testStore";
  private static final int TEST_VERSION = 1;
  private static final String TEST_TOPIC = "testStore_v1";

  private JobRequestHandler jobRequestHandler;
  private Admin admin;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    ControllerRequestHandlerDependencies dependencies = mock(ControllerRequestHandlerDependencies.class);
    when(dependencies.getAdmin()).thenReturn(admin);
    jobRequestHandler = new JobRequestHandler(dependencies);
  }

  @Test
  public void testGetJobStatusSuccess() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();

    Admin.OfflinePushStatusInfo statusInfo = new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED);
    when(admin.getOffLinePushStatus(TEST_CLUSTER, TEST_TOPIC, Optional.empty(), null, null, false))
        .thenReturn(statusInfo);

    GetJobStatusGrpcResponse response = jobRequestHandler.getJobStatus(request);

    assertNotNull(response);
    assertEquals(response.getStoreInfo().getClusterName(), TEST_CLUSTER);
    assertEquals(response.getStoreInfo().getStoreName(), TEST_STORE);
    assertEquals(response.getVersion(), TEST_VERSION);
    assertEquals(response.getStatus(), ExecutionStatus.COMPLETED.toString());
    verify(admin, times(1)).getOffLinePushStatus(TEST_CLUSTER, TEST_TOPIC, Optional.empty(), null, null, false);
  }

  @Test
  public void testGetJobStatusWithAllOptionalParameters() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request = GetJobStatusGrpcRequest.newBuilder()
        .setStoreInfo(storeInfo)
        .setVersion(TEST_VERSION)
        .setIncrementalPushVersion("inc-push-v1")
        .setTargetedRegions("dc-0,dc-1")
        .setIsTargetRegionPushWithDeferredSwap(true)
        .setRegion("dc-0")
        .build();

    Map<String, String> extraInfo = new HashMap<>();
    extraInfo.put("dc-0", "COMPLETED");
    extraInfo.put("dc-1", "STARTED");

    Map<String, String> extraDetails = new HashMap<>();
    extraDetails.put("dc-0", "All partitions completed");
    extraDetails.put("dc-1", "10/100 partitions completed");

    Map<String, Long> extraInfoUpdateTimestamp = new HashMap<>();
    extraInfoUpdateTimestamp.put("dc-0", 1234567890L);
    extraInfoUpdateTimestamp.put("dc-1", 1234567891L);

    Admin.OfflinePushStatusInfo statusInfo = new Admin.OfflinePushStatusInfo(
        ExecutionStatus.STARTED,
        1234567890L,
        extraInfo,
        "Push in progress",
        extraDetails,
        extraInfoUpdateTimestamp);

    when(admin.getOffLinePushStatus(TEST_CLUSTER, TEST_TOPIC, Optional.of("inc-push-v1"), "dc-0", "dc-0,dc-1", true))
        .thenReturn(statusInfo);

    GetJobStatusGrpcResponse response = jobRequestHandler.getJobStatus(request);

    assertNotNull(response);
    assertEquals(response.getStatus(), ExecutionStatus.STARTED.toString());
    assertEquals(response.getStatusDetails(), "Push in progress");
    assertEquals(response.getStatusUpdateTimestamp(), 1234567890L);
    assertEquals(response.getExtraInfoMap().size(), 2);
    assertEquals(response.getExtraInfoMap().get("dc-0"), "COMPLETED");
    assertEquals(response.getExtraInfoMap().get("dc-1"), "STARTED");
    assertEquals(response.getExtraDetailsMap().size(), 2);
    assertEquals(response.getExtraInfoUpdateTimestampMap().size(), 2);
  }

  @Test
  public void testGetJobStatusWithUncompletedPartitions() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();

    UncompletedReplica replica1 =
        new UncompletedReplica("instance-1", ExecutionStatus.STARTED, 1000L, "Processing records");
    UncompletedReplica replica2 = new UncompletedReplica("instance-2", ExecutionStatus.PROGRESS, 2000L, null);
    List<UncompletedReplica> replicas = Arrays.asList(replica1, replica2);
    UncompletedPartition partition = new UncompletedPartition(0, replicas);

    Admin.OfflinePushStatusInfo statusInfo = new Admin.OfflinePushStatusInfo(ExecutionStatus.STARTED);
    statusInfo.setUncompletedPartitions(Collections.singletonList(partition));

    when(admin.getOffLinePushStatus(TEST_CLUSTER, TEST_TOPIC, Optional.empty(), null, null, false))
        .thenReturn(statusInfo);

    GetJobStatusGrpcResponse response = jobRequestHandler.getJobStatus(request);

    assertNotNull(response);
    assertEquals(response.getUncompletedPartitionsCount(), 1);
    assertEquals(response.getUncompletedPartitions(0).getPartitionId(), 0);
    assertEquals(response.getUncompletedPartitions(0).getUncompletedReplicasCount(), 2);
    assertEquals(response.getUncompletedPartitions(0).getUncompletedReplicas(0).getInstanceId(), "instance-1");
    assertEquals(
        response.getUncompletedPartitions(0).getUncompletedReplicas(0).getStatus(),
        ExecutionStatus.STARTED.toString());
    assertEquals(response.getUncompletedPartitions(0).getUncompletedReplicas(0).getCurrentOffset(), 1000L);
    assertEquals(
        response.getUncompletedPartitions(0).getUncompletedReplicas(0).getStatusDetails(),
        "Processing records");
    assertFalse(response.getUncompletedPartitions(0).getUncompletedReplicas(1).hasStatusDetails());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Cluster name is mandatory parameter")
  public void testGetJobStatusMissingClusterName() {
    ClusterStoreGrpcInfo storeInfo = ClusterStoreGrpcInfo.newBuilder().setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();

    jobRequestHandler.getJobStatus(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Store name is mandatory parameter")
  public void testGetJobStatusMissingStoreName() {
    ClusterStoreGrpcInfo storeInfo = ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();

    jobRequestHandler.getJobStatus(request);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Version number must be a positive integer")
  public void testGetJobStatusInvalidVersion() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(0).build();

    jobRequestHandler.getJobStatus(request);
  }

  @Test
  public void testGetJobStatusWithNullExtraFields() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();

    Admin.OfflinePushStatusInfo statusInfo =
        new Admin.OfflinePushStatusInfo(ExecutionStatus.COMPLETED, null, null, null, null, null);

    when(admin.getOffLinePushStatus(TEST_CLUSTER, TEST_TOPIC, Optional.empty(), null, null, false))
        .thenReturn(statusInfo);

    GetJobStatusGrpcResponse response = jobRequestHandler.getJobStatus(request);

    assertNotNull(response);
    assertEquals(response.getStatus(), ExecutionStatus.COMPLETED.toString());
    assertFalse(response.hasStatusDetails());
    assertFalse(response.hasStatusUpdateTimestamp());
    assertTrue(response.getExtraInfoMap().isEmpty());
    assertTrue(response.getExtraDetailsMap().isEmpty());
    assertTrue(response.getExtraInfoUpdateTimestampMap().isEmpty());
    assertTrue(response.getUncompletedPartitionsList().isEmpty());
  }
}
