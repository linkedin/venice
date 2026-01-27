package com.linkedin.venice.controller.grpc.server;

import static com.linkedin.venice.controller.server.VeniceRouteHandler.ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.server.ClusterAdminOpsRequestHandler;
import com.linkedin.venice.controller.server.VeniceControllerAccessManager;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminCommandExecutionStatusGrpcResponse;
import com.linkedin.venice.protocols.controller.AdminTopicGrpcMetadata;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.controller.AdminTopicMetadataGrpcResponse;
import com.linkedin.venice.protocols.controller.ClusterAdminOpsGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.ClusterAdminOpsGrpcServiceGrpc.ClusterAdminOpsGrpcServiceBlockingStub;
import com.linkedin.venice.protocols.controller.IsStoreMigrationAllowedGrpcRequest;
import com.linkedin.venice.protocols.controller.IsStoreMigrationAllowedGrpcResponse;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcRequest;
import com.linkedin.venice.protocols.controller.LastSuccessfulAdminCommandExecutionGrpcResponse;
import com.linkedin.venice.protocols.controller.UpdateAdminOperationProtocolVersionGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAdminTopicMetadataGrpcRequest;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ClusterAdminOpsGrpcServiceImplTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";
  private static final long EXECUTION_ID = 12345L;

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private ClusterAdminOpsRequestHandler requestHandler;
  private ClusterAdminOpsGrpcServiceBlockingStub blockingStub;
  private VeniceControllerAccessManager accessManager;

  @BeforeMethod
  public void setUp() throws Exception {
    accessManager = mock(VeniceControllerAccessManager.class);
    requestHandler = mock(ClusterAdminOpsRequestHandler.class);

    String serverName = InProcessServerBuilder.generateName();
    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new ClusterAdminOpsGrpcServiceImpl(requestHandler, accessManager))
        .build()
        .start();

    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    blockingStub = ClusterAdminOpsGrpcServiceGrpc.newBlockingStub(grpcChannel);
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
  public void testGetAdminCommandExecutionStatusSuccess() {
    AdminCommandExecutionStatusGrpcResponse response = AdminCommandExecutionStatusGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setAdminCommandExecutionId(EXECUTION_ID)
        .build();
    doReturn(response).when(requestHandler)
        .getAdminCommandExecutionStatus(any(AdminCommandExecutionStatusGrpcRequest.class));

    AdminCommandExecutionStatusGrpcRequest request = AdminCommandExecutionStatusGrpcRequest.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setAdminCommandExecutionId(EXECUTION_ID)
        .build();

    AdminCommandExecutionStatusGrpcResponse actualResponse = blockingStub.getAdminCommandExecutionStatus(request);
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER);
    assertEquals(actualResponse.getAdminCommandExecutionId(), EXECUTION_ID);
  }

  @Test
  public void testGetAdminCommandExecutionStatusError() {
    AdminCommandExecutionStatusGrpcRequest request = AdminCommandExecutionStatusGrpcRequest.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setAdminCommandExecutionId(EXECUTION_ID)
        .build();

    doThrow(new VeniceException("Error")).when(requestHandler)
        .getAdminCommandExecutionStatus(any(AdminCommandExecutionStatusGrpcRequest.class));

    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.getAdminCommandExecutionStatus(request));
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
  }

  @Test
  public void testGetLastSuccessfulAdminCommandExecutionIdSuccess() {
    LastSuccessfulAdminCommandExecutionGrpcResponse response =
        LastSuccessfulAdminCommandExecutionGrpcResponse.newBuilder()
            .setClusterName(TEST_CLUSTER)
            .setLastSuccessfulAdminCommandExecutionId(EXECUTION_ID)
            .build();
    doReturn(response).when(requestHandler)
        .getLastSucceedExecutionId(any(LastSuccessfulAdminCommandExecutionGrpcRequest.class));

    LastSuccessfulAdminCommandExecutionGrpcRequest request =
        LastSuccessfulAdminCommandExecutionGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();

    LastSuccessfulAdminCommandExecutionGrpcResponse actualResponse =
        blockingStub.getLastSuccessfulAdminCommandExecutionId(request);
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER);
    assertEquals(actualResponse.getLastSuccessfulAdminCommandExecutionId(), EXECUTION_ID);
  }

  @Test
  public void testGetAdminTopicMetadataSuccess() {
    AdminTopicGrpcMetadata metadata =
        AdminTopicGrpcMetadata.newBuilder().setClusterName(TEST_CLUSTER).setExecutionId(EXECUTION_ID).build();
    AdminTopicMetadataGrpcResponse response = AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(metadata).build();
    doReturn(response).when(requestHandler).getAdminTopicMetadata(any(AdminTopicMetadataGrpcRequest.class));

    AdminTopicMetadataGrpcRequest request =
        AdminTopicMetadataGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();

    AdminTopicMetadataGrpcResponse actualResponse = blockingStub.getAdminTopicMetadata(request);
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getMetadata().getClusterName(), TEST_CLUSTER);
    assertEquals(actualResponse.getMetadata().getExecutionId(), EXECUTION_ID);
  }

  @Test
  public void testUpdateAdminTopicMetadataSuccess() {
    AdminTopicGrpcMetadata.Builder adminTopicGrpcMetadataBuilder = AdminTopicGrpcMetadata.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setExecutionId(EXECUTION_ID)
        .setOffset(100L)
        .setUpstreamOffset(-1L);
    AdminTopicMetadataGrpcResponse response =
        AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(adminTopicGrpcMetadataBuilder.build()).build();
    doReturn(response).when(requestHandler).updateAdminTopicMetadata(any(UpdateAdminTopicMetadataGrpcRequest.class));
    doReturn(true).when(accessManager).isAllowListUser(anyString(), any());

    UpdateAdminTopicMetadataGrpcRequest request = UpdateAdminTopicMetadataGrpcRequest.newBuilder()
        .setMetadata(
            AdminTopicGrpcMetadata.newBuilder()
                .setClusterName(TEST_CLUSTER)
                .setExecutionId(EXECUTION_ID)
                .setOffset(100L)
                .setUpstreamOffset(-1L))
        .build();

    AdminTopicMetadataGrpcResponse actualResponse = blockingStub.updateAdminTopicMetadata(request);
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getMetadata().getClusterName(), TEST_CLUSTER);
    assertEquals(actualResponse.getMetadata().getExecutionId(), EXECUTION_ID);
    assertEquals(actualResponse.getMetadata().getOffset(), 100L);
    assertEquals(actualResponse.getMetadata().getUpstreamOffset(), -1L);
    // Since store name is not provided in the request, no store name will be returned in the response
    assertFalse(actualResponse.getMetadata().hasStoreName());
  }

  @Test
  public void testUpdateAdminTopicMetadataUnauthorized() {
    UpdateAdminTopicMetadataGrpcRequest request = UpdateAdminTopicMetadataGrpcRequest.newBuilder()
        .setMetadata(AdminTopicGrpcMetadata.newBuilder().setClusterName(TEST_CLUSTER).setExecutionId(EXECUTION_ID))
        .build();
    doReturn(false).when(accessManager).isAllowListUser(anyString(), any());
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.updateAdminTopicMetadata(request));
    assertEquals(e.getStatus().getCode(), Status.PERMISSION_DENIED.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertTrue(
        errorInfo.getErrorMessage().contains(ACL_CHECK_FAILURE_WARN_MESSAGE_PREFIX),
        "Actual error message: " + errorInfo.getErrorMessage());
  }

  @Test
  public void testUpdateAdminOperationProtocolVersionSuccess() {
    AdminTopicGrpcMetadata.Builder adminTopicGrpcMetadataBuilder =
        AdminTopicGrpcMetadata.newBuilder().setClusterName(TEST_CLUSTER).setAdminOperationProtocolVersion(1L);
    AdminTopicMetadataGrpcResponse response =
        AdminTopicMetadataGrpcResponse.newBuilder().setMetadata(adminTopicGrpcMetadataBuilder.build()).build();
    doReturn(response).when(requestHandler)
        .updateAdminOperationProtocolVersion(any(UpdateAdminOperationProtocolVersionGrpcRequest.class));
    doReturn(true).when(accessManager).isAllowListUser(anyString(), any());

    UpdateAdminOperationProtocolVersionGrpcRequest request = UpdateAdminOperationProtocolVersionGrpcRequest.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setAdminOperationProtocolVersion(1L)
        .build();

    AdminTopicMetadataGrpcResponse actualResponse = blockingStub.updateAdminOperationProtocolVersion(request);
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getMetadata().getClusterName(), TEST_CLUSTER);
    assertEquals(actualResponse.getMetadata().getAdminOperationProtocolVersion(), 1L);
  }

  @Test
  public void testIsStoreMigrationAllowedSuccess() {
    IsStoreMigrationAllowedGrpcResponse response = IsStoreMigrationAllowedGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setStoreMigrationAllowed(true)
        .build();
    doReturn(response).when(requestHandler).isStoreMigrationAllowed(any(IsStoreMigrationAllowedGrpcRequest.class));

    IsStoreMigrationAllowedGrpcRequest request =
        IsStoreMigrationAllowedGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();

    IsStoreMigrationAllowedGrpcResponse actualResponse = blockingStub.isStoreMigrationAllowed(request);
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER);
    assertTrue(actualResponse.getStoreMigrationAllowed());
  }

  @Test
  public void testIsStoreMigrationAllowedReturnsFalse() {
    IsStoreMigrationAllowedGrpcResponse response = IsStoreMigrationAllowedGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setStoreMigrationAllowed(false)
        .build();
    doReturn(response).when(requestHandler).isStoreMigrationAllowed(any(IsStoreMigrationAllowedGrpcRequest.class));

    IsStoreMigrationAllowedGrpcRequest request =
        IsStoreMigrationAllowedGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();

    IsStoreMigrationAllowedGrpcResponse actualResponse = blockingStub.isStoreMigrationAllowed(request);
    assertNotNull(actualResponse);
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER);
    assertFalse(actualResponse.getStoreMigrationAllowed());
  }

  @Test
  public void testIsStoreMigrationAllowedError() {
    IsStoreMigrationAllowedGrpcRequest request =
        IsStoreMigrationAllowedGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();

    doThrow(new VeniceException("Error checking migration allowed")).when(requestHandler)
        .isStoreMigrationAllowed(any(IsStoreMigrationAllowedGrpcRequest.class));

    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.isStoreMigrationAllowed(request));
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
  }
}
