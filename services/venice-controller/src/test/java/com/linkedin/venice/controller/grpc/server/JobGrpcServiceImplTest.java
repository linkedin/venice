package com.linkedin.venice.controller.grpc.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.server.JobRequestHandler;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.GetJobStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.JobGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.JobGrpcServiceGrpc.JobGrpcServiceBlockingStub;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class JobGrpcServiceImplTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";
  private static final int TEST_VERSION = 1;

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private JobRequestHandler jobRequestHandler;
  private JobGrpcServiceBlockingStub blockingStub;

  @BeforeMethod
  public void setUp() throws Exception {
    jobRequestHandler = mock(JobRequestHandler.class);

    String serverName = InProcessServerBuilder.generateName();

    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new JobGrpcServiceImpl(jobRequestHandler))
        .build()
        .start();

    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    blockingStub = JobGrpcServiceGrpc.newBlockingStub(grpcChannel);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (grpcServer != null) {
      grpcServer.shutdown();
    }
    if (grpcChannel != null) {
      grpcChannel.shutdown();
    }
  }

  @Test
  public void testGetJobStatusReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();
    GetJobStatusGrpcResponse expectedResponse = GetJobStatusGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setVersion(TEST_VERSION)
        .setStatus(ExecutionStatus.COMPLETED.toString())
        .build();

    doReturn(expectedResponse).when(jobRequestHandler).getJobStatus(any(GetJobStatusGrpcRequest.class));

    GetJobStatusGrpcResponse actualResponse = blockingStub.getJobStatus(request);

    assertNotNull(actualResponse);
    assertEquals(actualResponse.getStoreInfo().getClusterName(), TEST_CLUSTER);
    assertEquals(actualResponse.getStoreInfo().getStoreName(), TEST_STORE);
    assertEquals(actualResponse.getVersion(), TEST_VERSION);
    assertEquals(actualResponse.getStatus(), ExecutionStatus.COMPLETED.toString());
  }

  @Test
  public void testGetJobStatusReturnsStartedStatus() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();
    GetJobStatusGrpcResponse expectedResponse = GetJobStatusGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setVersion(TEST_VERSION)
        .setStatus(ExecutionStatus.STARTED.toString())
        .setStatusDetails("Push in progress")
        .setStatusUpdateTimestamp(1234567890L)
        .putExtraInfo("dc-0", "STARTED")
        .build();

    doReturn(expectedResponse).when(jobRequestHandler).getJobStatus(any(GetJobStatusGrpcRequest.class));

    GetJobStatusGrpcResponse actualResponse = blockingStub.getJobStatus(request);

    assertNotNull(actualResponse);
    assertEquals(actualResponse.getStatus(), ExecutionStatus.STARTED.toString());
    assertEquals(actualResponse.getStatusDetails(), "Push in progress");
    assertEquals(actualResponse.getStatusUpdateTimestamp(), 1234567890L);
    assertEquals(actualResponse.getExtraInfoMap().get("dc-0"), "STARTED");
  }

  @Test
  public void testGetJobStatusReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(TEST_VERSION).build();

    doThrow(new VeniceException("Failed to get job status")).when(jobRequestHandler)
        .getJobStatus(any(GetJobStatusGrpcRequest.class));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getJobStatus(request));

    assertNotNull(e.getStatus());
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("Failed to get job status"));
  }

  @Test
  public void testGetJobStatusReturnsInvalidArgumentError() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetJobStatusGrpcRequest request =
        GetJobStatusGrpcRequest.newBuilder().setStoreInfo(storeInfo).setVersion(0).build();

    doThrow(new IllegalArgumentException("Version number must be a positive integer")).when(jobRequestHandler)
        .getJobStatus(any(GetJobStatusGrpcRequest.class));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getJobStatus(request));

    assertNotNull(e.getStatus());
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Version number must be a positive integer"));
  }

  @Test
  public void testGetJobStatusWithOptionalParameters() {
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
    GetJobStatusGrpcResponse expectedResponse = GetJobStatusGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setVersion(TEST_VERSION)
        .setStatus(ExecutionStatus.COMPLETED.toString())
        .build();

    doReturn(expectedResponse).when(jobRequestHandler).getJobStatus(any(GetJobStatusGrpcRequest.class));

    GetJobStatusGrpcResponse actualResponse = blockingStub.getJobStatus(request);

    assertNotNull(actualResponse);
    assertEquals(actualResponse.getStatus(), ExecutionStatus.COMPLETED.toString());
  }
}
