package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.transport.GrpcRequestResponseConverter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.controller.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceControllerGrpcServiceImplTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";
  private static final String D2_TEST_SERVICE = "d2://test-service";
  private static final String D2_TEST_SERVER = "d2://test-server";
  private static final String HTTP_URL = "http://localhost:8080";
  private static final String HTTPS_URL = "https://localhost:8081";
  private static final String GRPC_URL = "grpc://localhost:8082";
  private static final String SECURE_GRPC_URL = "grpcs://localhost:8083";
  private static final String OWNER = "test-owner";
  private static final String KEY_SCHEMA = "int";
  private static final String VALUE_SCHEMA = "string";

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private VeniceControllerRequestHandler requestHandler;
  private VeniceControllerGrpcServiceBlockingStub blockingStub;
  private VeniceControllerAccessManager controllerAccessManager;

  @BeforeMethod
  public void setUp() throws Exception {
    requestHandler = mock(VeniceControllerRequestHandler.class);
    controllerAccessManager = mock(VeniceControllerAccessManager.class);
    when(requestHandler.getControllerAccessManager()).thenReturn(controllerAccessManager);

    // Create a unique server name for the in-process server
    String serverName = InProcessServerBuilder.generateName();

    // Start the gRPC server in-process
    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new VeniceControllerGrpcServiceImpl(requestHandler))
        .build()
        .start();

    // Create a channel to communicate with the server
    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    // Create a blocking stub to make calls to the server
    blockingStub = VeniceControllerGrpcServiceGrpc.newBlockingStub(grpcChannel);
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
  public void testGetLeaderController() {
    // Case 1: Successful response
    LeaderControllerGrpcResponse response = LeaderControllerGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .setHttpUrl(HTTP_URL)
        .setHttpsUrl(HTTPS_URL)
        .setGrpcUrl(GRPC_URL)
        .setSecureGrpcUrl(SECURE_GRPC_URL)
        .build();
    doReturn(response).when(requestHandler).getLeaderControllerDetails(any(LeaderControllerGrpcRequest.class));

    LeaderControllerGrpcRequest request = LeaderControllerGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    LeaderControllerGrpcResponse actualResponse = blockingStub.getLeaderController(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getHttpUrl(), HTTP_URL, "HTTP URL should match");
    assertEquals(actualResponse.getHttpsUrl(), HTTPS_URL, "HTTPS URL should match");
    assertEquals(actualResponse.getGrpcUrl(), GRPC_URL, "gRPC URL should match");
    assertEquals(actualResponse.getSecureGrpcUrl(), SECURE_GRPC_URL, "Secure gRPC URL should match");

    // Case 2: Bad request as cluster name is missing
    doThrow(new IllegalArgumentException("Cluster name is required for leader controller discovery"))
        .when(requestHandler)
        .getLeaderControllerDetails(any(LeaderControllerGrpcRequest.class));
    LeaderControllerGrpcRequest requestWithoutClusterName = LeaderControllerGrpcRequest.newBuilder().build();
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.getLeaderController(requestWithoutClusterName));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());

    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertFalse(errorInfo.hasStoreName(), "Store name should not be present in the error info");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Cluster name is required for leader controller discovery"));

    // Case 3: requestHandler throws an exception
    doThrow(new VeniceException("Failed to get leader controller")).when(requestHandler)
        .getLeaderControllerDetails(any(LeaderControllerGrpcRequest.class));
    StatusRuntimeException e2 =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.getLeaderController(request));
    assertNotNull(e2.getStatus(), "Status should not be null");
    assertEquals(e2.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo2 = GrpcRequestResponseConverter.parseControllerGrpcError(e2);
    assertNotNull(errorInfo2, "Error info should not be null");
    assertTrue(errorInfo2.hasClusterName(), "Cluster name should be present in the error info");
    assertEquals(errorInfo2.getClusterName(), TEST_CLUSTER);
    assertFalse(errorInfo2.hasStoreName(), "Store name should not be present in the error info");
    assertEquals(errorInfo2.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo2.getErrorMessage().contains("Failed to get leader controller"));
  }

  @Test
  public void testDiscoverClusterForStore() {
    // Case 1: Successful response
    DiscoverClusterGrpcResponse response = DiscoverClusterGrpcResponse.newBuilder()
        .setStoreName(TEST_STORE)
        .setClusterName(TEST_CLUSTER)
        .setD2Service(D2_TEST_SERVICE)
        .setServerD2Service(D2_TEST_SERVER)
        .build();
    doReturn(response).when(requestHandler).discoverCluster(any(DiscoverClusterGrpcRequest.class));
    DiscoverClusterGrpcRequest request = DiscoverClusterGrpcRequest.newBuilder().setStoreName(TEST_STORE).build();
    DiscoverClusterGrpcResponse actualResponse = blockingStub.discoverClusterForStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getStoreName(), TEST_STORE, "Store name should match");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getD2Service(), D2_TEST_SERVICE, "D2 service should match");
    assertEquals(actualResponse.getServerD2Service(), D2_TEST_SERVER, "Server D2 service should match");

    // Case 2: Bad request as store name is missing
    doThrow(new IllegalArgumentException("Store name is required for cluster discovery")).when(requestHandler)
        .discoverCluster(any(DiscoverClusterGrpcRequest.class));
    DiscoverClusterGrpcRequest requestWithoutStoreName = DiscoverClusterGrpcRequest.newBuilder().build();
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.discoverClusterForStore(requestWithoutStoreName));
    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertFalse(errorInfo.hasClusterName(), "Cluster name should not be present in the error info");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Store name is required for cluster discovery"));

    // Case 3: requestHandler throws an exception
    doThrow(new VeniceException("Failed to discover cluster")).when(requestHandler)
        .discoverCluster(any(DiscoverClusterGrpcRequest.class));
    StatusRuntimeException e2 =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.discoverClusterForStore(request));
    assertNotNull(e2.getStatus(), "Status should not be null");
    assertEquals(e2.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo2 = GrpcRequestResponseConverter.parseControllerGrpcError(e2);
    assertNotNull(errorInfo2, "Error info should not be null");
    assertFalse(errorInfo2.hasClusterName(), "Cluster name should not be present in the error info");
    assertEquals(errorInfo2.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo2.getErrorMessage().contains("Failed to discover cluster"));
  }

  @Test
  public void testCreateStore() {
    when(controllerAccessManager.isAllowListUser(anyString(), any())).thenReturn(true);
    CreateStoreGrpcResponse response = CreateStoreGrpcResponse.newBuilder()
        .setClusterStoreInfo(
            ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setOwner(OWNER)
        .build();
    // Case 1: Successful response
    doReturn(response).when(requestHandler).createStore(any(CreateStoreGrpcRequest.class));
    CreateStoreGrpcRequest request = CreateStoreGrpcRequest.newBuilder()
        .setClusterStoreInfo(
            ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build())
        .setOwner(OWNER)
        .setKeySchema(KEY_SCHEMA)
        .setValueSchema(VALUE_SCHEMA)
        .build();
    CreateStoreGrpcResponse actualResponse = blockingStub.createStore(request);
    assertNotNull(actualResponse, "Response should not be null");
    assertNotNull(actualResponse.getClusterStoreInfo(), "ClusterStoreInfo should not be null");
    assertEquals(actualResponse.getClusterStoreInfo().getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getClusterStoreInfo().getStoreName(), TEST_STORE, "Store name should match");

    // Case 2: Bad request as cluster name is missing
    CreateStoreGrpcRequest requestWithoutClusterName = CreateStoreGrpcRequest.newBuilder()
        .setOwner(OWNER)
        .setKeySchema(KEY_SCHEMA)
        .setValueSchema(VALUE_SCHEMA)
        .build();
    doThrow(new IllegalArgumentException("The request is missing the cluster_name")).when(requestHandler)
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
    doThrow(new VeniceException("Failed to create store")).when(requestHandler)
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
        "Acual: " + errorInfo4.getErrorMessage());
  }
}
