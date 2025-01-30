package com.linkedin.venice.controller.grpc.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.server.StoreRequestHandler;
import com.linkedin.venice.controller.server.VeniceControllerAccessManager;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ResourceCleanupCheckGrpcResponse;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
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


public class StoreGrpcServiceImplTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";
  private static final String OWNER = "test-owner";
  private static final String KEY_SCHEMA = "int";
  private static final String VALUE_SCHEMA = "string";

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private StoreRequestHandler storeRequestHandler;
  private StoreGrpcServiceBlockingStub blockingStub;
  private VeniceControllerAccessManager controllerAccessManager;

  @BeforeMethod
  public void setUp() throws Exception {
    controllerAccessManager = mock(VeniceControllerAccessManager.class);
    storeRequestHandler = mock(StoreRequestHandler.class);

    // Create a unique server name for the in-process server
    String serverName = InProcessServerBuilder.generateName();

    // Start the gRPC server in-process
    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new StoreGrpcServiceImpl(storeRequestHandler, controllerAccessManager))
        .build()
        .start();

    // Create a channel to communicate with the server
    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    // Create a blocking stub to make calls to the server
    blockingStub = StoreGrpcServiceGrpc.newBlockingStub(grpcChannel);
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
  public void testCreateStore() {
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(true);
    CreateStoreGrpcResponse response = CreateStoreGrpcResponse.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setOwner(OWNER)
        .build();
    // Case 1: Successful response
    doReturn(response).when(storeRequestHandler).createStore(any(CreateStoreGrpcRequest.class));
    CreateStoreGrpcRequest request = CreateStoreGrpcRequest.newBuilder()
        .setStoreInfo(ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setOwner(OWNER)
        .setKeySchema(KEY_SCHEMA)
        .setValueSchema(VALUE_SCHEMA)
        .build();
    CreateStoreGrpcResponse actualResponse = blockingStub.createStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertNotNull(actualResponse.getStoreInfo(), "ClusterStoreInfo should not be null");
    assertEquals(actualResponse.getStoreInfo().getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getStoreInfo().getStoreName(), TEST_STORE, "Store name should match");

    // Case 2: Bad request as cluster name is missing
    CreateStoreGrpcRequest requestWithoutClusterName = CreateStoreGrpcRequest.newBuilder()
        .setOwner(OWNER)
        .setKeySchema(KEY_SCHEMA)
        .setValueSchema(VALUE_SCHEMA)
        .build();
    doThrow(new IllegalArgumentException("The request is missing the cluster_name")).when(storeRequestHandler)
        .createStore(any(CreateStoreGrpcRequest.class));
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.createStore(requestWithoutClusterName));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertNotNull(errorInfo, "Error info should not be null");
    assertTrue(errorInfo.getErrorMessage().contains("The request is missing the cluster_name"));

    // Case 3: requestHandler throws an exception
    doThrow(new VeniceException("Failed to create store")).when(storeRequestHandler)
        .createStore(any(CreateStoreGrpcRequest.class));
    StatusRuntimeException e3 = expectThrows(StatusRuntimeException.class, () -> blockingStub.createStore(request));
    assertNotNull(e3.getStatus(), "Status should not be null");
    assertEquals(e3.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo3 = GrpcRequestResponseConverter.parseControllerGrpcError(e3);
    assertNotNull(errorInfo3, "Error info should not be null");
    assertEquals(errorInfo3.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo3.getErrorMessage().contains("Failed to create store"));

    // Case 4: Permission denied
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(false);
    StatusRuntimeException e4 = expectThrows(StatusRuntimeException.class, () -> blockingStub.createStore(request));
    assertNotNull(e4.getStatus(), "Status should not be null");
    assertEquals(e4.getStatus().getCode(), Status.PERMISSION_DENIED.getCode());
    VeniceControllerGrpcErrorInfo errorInfo4 = GrpcRequestResponseConverter.parseControllerGrpcError(e4);
    assertNotNull(errorInfo4, "Error info should not be null");
    assertEquals(errorInfo4.getErrorType(), ControllerGrpcErrorType.UNAUTHORIZED);
    assertTrue(
        errorInfo4.getErrorMessage().contains("Only admin users are allowed to run"),
        "Actual: " + errorInfo4.getErrorMessage());
  }

  @Test
  public void testUpdateAclForStoreReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    UpdateAclForStoreGrpcRequest request = UpdateAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    UpdateAclForStoreGrpcResponse response = UpdateAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.updateAclForStore(any(UpdateAclForStoreGrpcRequest.class))).thenReturn(response);
    UpdateAclForStoreGrpcResponse actualResponse = blockingStub.updateAclForStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse, response, "Response should match");
  }

  @Test
  public void testUpdateAclForStoreReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    UpdateAclForStoreGrpcRequest request = UpdateAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.updateAclForStore(any(UpdateAclForStoreGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to update ACL"));
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.updateAclForStore(request));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertNotNull(errorInfo, "Error info should not be null");
    assertTrue(errorInfo.getErrorMessage().contains("Failed to update ACL"));
  }

  @Test
  public void testGetAclForStoreReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetAclForStoreGrpcRequest request = GetAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    GetAclForStoreGrpcResponse response = GetAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.getAclForStore(any(GetAclForStoreGrpcRequest.class))).thenReturn(response);
    GetAclForStoreGrpcResponse actualResponse = blockingStub.getAclForStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse, response, "Response should match");
  }

  @Test
  public void testGetAclForStoreReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetAclForStoreGrpcRequest request = GetAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.getAclForStore(any(GetAclForStoreGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to get ACL"));
    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getAclForStore(request));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertNotNull(errorInfo, "Error info should not be null");
    assertTrue(errorInfo.getErrorMessage().contains("Failed to get ACL"));
  }

  @Test
  public void testDeleteAclForStoreReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    DeleteAclForStoreGrpcRequest request = DeleteAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    DeleteAclForStoreGrpcResponse response = DeleteAclForStoreGrpcResponse.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.deleteAclForStore(any(DeleteAclForStoreGrpcRequest.class))).thenReturn(response);
    DeleteAclForStoreGrpcResponse actualResponse = blockingStub.deleteAclForStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse, response, "Response should match");
  }

  @Test
  public void testDeleteAclForStoreReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    DeleteAclForStoreGrpcRequest request = DeleteAclForStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.deleteAclForStore(any(DeleteAclForStoreGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to delete ACL"));
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.deleteAclForStore(request));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertNotNull(errorInfo, "Error info should not be null");
    assertTrue(errorInfo.getErrorMessage().contains("Failed to delete ACL"));
  }

  @Test
  public void testCheckResourceCleanupForStoreCreationSuccess() {
    ClusterStoreGrpcInfo request =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();

    // No lingering resources
    doNothing().when(storeRequestHandler).checkResourceCleanupForStoreCreation(any(ClusterStoreGrpcInfo.class));
    ResourceCleanupCheckGrpcResponse response = blockingStub.checkResourceCleanupForStoreCreation(request);
    assertNotNull(response, "Response should not be null");
    assertEquals(response.getStoreInfo(), request, "Store info should match");
    assertFalse(response.getHasLingeringResources(), "Lingering resources should be false");

    // Lingering resources
    String exceptionMessage = "Lingering resources detected";
    doThrow(new VeniceException(exceptionMessage)).when(storeRequestHandler)
        .checkResourceCleanupForStoreCreation(any(ClusterStoreGrpcInfo.class));
    response = blockingStub.checkResourceCleanupForStoreCreation(request);
    assertNotNull(response, "Response should not be null");
    assertEquals(response.getStoreInfo(), request, "Store info should match");
    assertTrue(response.getHasLingeringResources(), "Lingering resources should be true");
    assertEquals(response.getDescription(), exceptionMessage, "Description should match");

    // null exception message
    doThrow(new VeniceException()).when(storeRequestHandler)
        .checkResourceCleanupForStoreCreation(any(ClusterStoreGrpcInfo.class));
    response = blockingStub.checkResourceCleanupForStoreCreation(request);
    assertNotNull(response, "Response should not be null");
    assertEquals(response.getStoreInfo(), request, "Store info should match");
    assertTrue(response.getHasLingeringResources(), "Lingering resources should be true");
    assertTrue(response.getDescription().isEmpty(), "Description should be empty");
  }
}
