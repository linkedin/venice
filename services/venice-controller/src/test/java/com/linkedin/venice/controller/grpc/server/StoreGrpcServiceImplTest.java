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
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.DeleteAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetRepushInfoGrpcRequest;
import com.linkedin.venice.protocols.controller.GetRepushInfoGrpcResponse;
import com.linkedin.venice.protocols.controller.GetStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.GetStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.GetStoreStatusRequest;
import com.linkedin.venice.protocols.controller.GetStoreStatusResponse;
import com.linkedin.venice.protocols.controller.ListStoresGrpcRequest;
import com.linkedin.venice.protocols.controller.ListStoresGrpcResponse;
import com.linkedin.venice.protocols.controller.ResourceCleanupCheckGrpcResponse;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.StoreGrpcServiceGrpc.StoreGrpcServiceBlockingStub;
import com.linkedin.venice.protocols.controller.StoreStatus;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.UpdateAclForStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcRequest;
import com.linkedin.venice.protocols.controller.ValidateStoreDeletedGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import com.linkedin.venice.protocols.controller.VersionStatusGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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

  @Test
  public void testValidateStoreDeletedReturnsSuccessfulResponseWhenStoreIsDeleted() {
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(true);
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    ValidateStoreDeletedGrpcRequest request =
        ValidateStoreDeletedGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    ValidateStoreDeletedGrpcResponse response =
        ValidateStoreDeletedGrpcResponse.newBuilder().setStoreInfo(storeInfo).setStoreDeleted(true).build();
    when(storeRequestHandler.validateStoreDeleted(any(ValidateStoreDeletedGrpcRequest.class))).thenReturn(response);

    ValidateStoreDeletedGrpcResponse actualResponse = blockingStub.validateStoreDeleted(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getStoreInfo(), storeInfo, "Store info should match");
    assertTrue(actualResponse.getStoreDeleted(), "Store should be marked as deleted");
    assertFalse(actualResponse.hasReason(), "Reason should not be set when store is deleted");
  }

  @Test
  public void testValidateStoreDeletedReturnsSuccessfulResponseWhenStoreIsNotDeleted() {
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(true);
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    ValidateStoreDeletedGrpcRequest request =
        ValidateStoreDeletedGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    String reason = "Store config still exists in ZooKeeper";
    ValidateStoreDeletedGrpcResponse response = ValidateStoreDeletedGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setStoreDeleted(false)
        .setReason(reason)
        .build();
    when(storeRequestHandler.validateStoreDeleted(any(ValidateStoreDeletedGrpcRequest.class))).thenReturn(response);

    ValidateStoreDeletedGrpcResponse actualResponse = blockingStub.validateStoreDeleted(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getStoreInfo(), storeInfo, "Store info should match");
    assertFalse(actualResponse.getStoreDeleted(), "Store should be marked as not deleted");
    assertTrue(actualResponse.hasReason(), "Reason should be set when store is not deleted");
    assertEquals(actualResponse.getReason(), reason, "Reason should match");
  }

  @Test
  public void testValidateStoreDeletedReturnsErrorResponse() {
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(true);
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    ValidateStoreDeletedGrpcRequest request =
        ValidateStoreDeletedGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.validateStoreDeleted(any(ValidateStoreDeletedGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to validate store deletion"));

    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.validateStoreDeleted(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("Failed to validate store deletion"));
  }

  @Test
  public void testValidateStoreDeletedReturnsBadRequestForInvalidArgument() {
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(true);
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    ValidateStoreDeletedGrpcRequest request =
        ValidateStoreDeletedGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.validateStoreDeleted(any(ValidateStoreDeletedGrpcRequest.class)))
        .thenThrow(new IllegalArgumentException("Cluster name is mandatory parameter"));

    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.validateStoreDeleted(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Cluster name is mandatory parameter"));
  }

  @Test
  public void testValidateStoreDeletedReturnsPermissionDenied() {
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(false);
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    ValidateStoreDeletedGrpcRequest request =
        ValidateStoreDeletedGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.validateStoreDeleted(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.PERMISSION_DENIED.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.UNAUTHORIZED);
    assertTrue(
        errorInfo.getErrorMessage().contains("Only admin users are allowed to run"),
        "Actual: " + errorInfo.getErrorMessage());
  }

  @Test
  public void testListStoresReturnsSuccessfulResponse() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    ListStoresGrpcResponse expectedResponse = ListStoresGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .addAllStoreNames(Arrays.asList("store1", "store2", "store3"))
        .build();
    when(storeRequestHandler.listStores(any(ListStoresGrpcRequest.class))).thenReturn(expectedResponse);

    ListStoresGrpcResponse actualResponse = blockingStub.listStores(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getStoreNamesCount(), 3, "Should have 3 stores");
    assertEquals(actualResponse.getStoreNames(0), "store1", "First store should be store1");
    assertEquals(actualResponse.getStoreNames(1), "store2", "Second store should be store2");
    assertEquals(actualResponse.getStoreNames(2), "store3", "Third store should be store3");
  }

  @Test
  public void testListStoresReturnsErrorResponse() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    when(storeRequestHandler.listStores(any(ListStoresGrpcRequest.class)))
        .thenThrow(new VeniceException("Failed to list stores"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.listStores(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("Failed to list stores"));
  }

  @Test
  public void testListStoresReturnsBadRequestForMissingClusterName() {
    ListStoresGrpcRequest request = ListStoresGrpcRequest.newBuilder().build();
    when(storeRequestHandler.listStores(any(ListStoresGrpcRequest.class)))
        .thenThrow(new IllegalArgumentException("Cluster name is required"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.listStores(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Cluster name is required"));
  }

  @Test
  public void testListStoresWithFilters() {
    ListStoresGrpcRequest request =
        ListStoresGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).setIncludeSystemStores(false).build();
    ListStoresGrpcResponse expectedResponse = ListStoresGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .addAllStoreNames(Arrays.asList("store1"))
        .build();
    when(storeRequestHandler.listStores(any(ListStoresGrpcRequest.class))).thenReturn(expectedResponse);

    ListStoresGrpcResponse actualResponse = blockingStub.listStores(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getStoreNamesCount(), 1, "Should have 1 store after filtering");
  }

  @Test
  public void testGetStoreStatusesReturnsSuccessfulResponse() {
    GetStoreStatusRequest request = GetStoreStatusRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    Map<String, String> storeStatusMap = new HashMap<>();
    storeStatusMap.put("store1", "ONLINE");
    storeStatusMap.put("store2", "DEGRADED");
    storeStatusMap.put("store3", "UNAVAILABLE");
    when(storeRequestHandler.getStoreStatuses(TEST_CLUSTER)).thenReturn(storeStatusMap);

    GetStoreStatusResponse actualResponse = blockingStub.getStoreStatuses(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getStoreStatusesCount(), 3, "Should have 3 stores");

    // Verify each store status
    Map<String, String> responseMap = new HashMap<>();
    for (StoreStatus status: actualResponse.getStoreStatusesList()) {
      responseMap.put(status.getStoreName(), status.getStatus());
    }
    assertEquals(responseMap.get("store1"), "ONLINE", "store1 should be ONLINE");
    assertEquals(responseMap.get("store2"), "DEGRADED", "store2 should be DEGRADED");
    assertEquals(responseMap.get("store3"), "UNAVAILABLE", "store3 should be UNAVAILABLE");
  }

  @Test
  public void testGetStoreStatusesReturnsEmptyMapWhenNoStores() {
    GetStoreStatusRequest request = GetStoreStatusRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    when(storeRequestHandler.getStoreStatuses(TEST_CLUSTER)).thenReturn(new HashMap<>());

    GetStoreStatusResponse actualResponse = blockingStub.getStoreStatuses(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getStoreStatusesCount(), 0, "Should have 0 stores");
  }

  @Test
  public void testGetStoreStatusesReturnsErrorResponse() {
    GetStoreStatusRequest request = GetStoreStatusRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    when(storeRequestHandler.getStoreStatuses(TEST_CLUSTER))
        .thenThrow(new VeniceException("Failed to get store statuses"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getStoreStatuses(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("Failed to get store statuses"));
  }

  @Test
  public void testGetStoreStatusesReturnsBadRequestForMissingClusterName() {
    GetStoreStatusRequest request = GetStoreStatusRequest.newBuilder().build();
    when(storeRequestHandler.getStoreStatuses("")).thenThrow(new IllegalArgumentException("Cluster name is required"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getStoreStatuses(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Cluster name is required"));
  }

  @Test
  public void testGetRepushInfoReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetRepushInfoGrpcRequest request =
        GetRepushInfoGrpcRequest.newBuilder().setStoreInfo(storeInfo).setFabric("test-fabric").build();

    Version mockVersion = mock(Version.class);
    when(mockVersion.getNumber()).thenReturn(1);
    when(mockVersion.getCreatedTime()).thenReturn(123456789L);
    when(mockVersion.getStatus()).thenReturn(VersionStatus.ONLINE);
    when(mockVersion.getPushJobId()).thenReturn("test-push-job");
    when(mockVersion.getPartitionCount()).thenReturn(10);
    when(mockVersion.getReplicationFactor()).thenReturn(3);

    RepushInfo repushInfo = RepushInfo.createRepushInfo(mockVersion, "kafka.broker:9092", "d2-service", "zk-host");

    when(storeRequestHandler.getRepushInfo(anyString(), anyString(), any())).thenReturn(repushInfo);

    GetRepushInfoGrpcResponse actualResponse = blockingStub.getRepushInfo(request);

    assertNotNull(actualResponse);
    assertEquals(actualResponse.getStoreInfo(), storeInfo);
    assertEquals(actualResponse.getRepushInfo().getPubSubUrl(), "kafka.broker:9092");
    assertTrue(actualResponse.getRepushInfo().hasVersion());
    assertEquals(actualResponse.getRepushInfo().getVersion().getNumber(), 1);
    assertEquals(
        actualResponse.getRepushInfo().getVersion().getStatus(),
        VersionStatusGrpc.forNumber(VersionStatus.ONLINE.getValue()));
  }

  @Test
  public void testGetRepushInfoReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetRepushInfoGrpcRequest request = GetRepushInfoGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    when(storeRequestHandler.getRepushInfo(anyString(), anyString(), any()))
        .thenThrow(new VeniceException("Failed to get repush info"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getRepushInfo(request));

    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
  }

  @Test
  public void testGetRepushInfoWithoutFabric() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetRepushInfoGrpcRequest request = GetRepushInfoGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    RepushInfo repushInfo = RepushInfo.createRepushInfo(null, "another.kafka.broker:9092", null, null);

    when(storeRequestHandler.getRepushInfo(anyString(), anyString(), any())).thenReturn(repushInfo);

    GetRepushInfoGrpcResponse actualResponse = blockingStub.getRepushInfo(request);

    assertNotNull(actualResponse);
    assertEquals(actualResponse.getRepushInfo().getPubSubUrl(), "another.kafka.broker:9092");
    assertFalse(actualResponse.getRepushInfo().hasVersion());
  }

  @Test
  public void testGetStoreReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetStoreGrpcRequest request = GetStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();

    // Mock handler returning StoreInfo
    StoreInfo mockStoreInfo = new StoreInfo();
    mockStoreInfo.setName(TEST_STORE);
    mockStoreInfo.setOwner(OWNER);
    when(storeRequestHandler.getStore(TEST_CLUSTER, TEST_STORE)).thenReturn(mockStoreInfo);

    GetStoreGrpcResponse actualResponse = blockingStub.getStore(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getStoreInfo().getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getStoreInfo().getStoreName(), TEST_STORE, "Store name should match");
    assertTrue(actualResponse.getStoreInfoJson().contains(TEST_STORE), "Store info JSON should contain store name");
  }

  @Test
  public void testGetStoreReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetStoreGrpcRequest request = GetStoreGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(storeRequestHandler.getStore(TEST_CLUSTER, TEST_STORE)).thenThrow(new VeniceNoStoreException(TEST_STORE));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getStore(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.NOT_FOUND.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.STORE_NOT_FOUND);
    assertTrue(errorInfo.getErrorMessage().contains(TEST_STORE));
  }
}
