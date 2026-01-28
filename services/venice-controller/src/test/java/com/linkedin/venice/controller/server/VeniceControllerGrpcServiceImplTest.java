package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcRequest;
import com.linkedin.venice.protocols.controller.DiscoverClusterGrpcResponse;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.ListChildClustersGrpcRequest;
import com.linkedin.venice.protocols.controller.ListChildClustersGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.HashMap;
import java.util.Map;
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

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private VeniceControllerRequestHandler requestHandler;
  private VeniceControllerGrpcServiceBlockingStub blockingStub;

  @BeforeMethod
  public void setUp() throws Exception {
    requestHandler = mock(VeniceControllerRequestHandler.class);

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
  public void testListChildClustersReturnsSuccessfulResponse() {
    Map<String, String> childUrlMap = new HashMap<>();
    childUrlMap.put("dc1", "http://dc1-controller:8080");
    childUrlMap.put("dc2", "http://dc2-controller:8080");
    Map<String, String> childD2Map = new HashMap<>();
    childD2Map.put("dc1", "d2://dc1-controller");
    childD2Map.put("dc2", "d2://dc2-controller");

    ListChildClustersGrpcResponse response = ListChildClustersGrpcResponse.newBuilder()
        .setClusterName(TEST_CLUSTER)
        .putAllChildDataCenterControllerUrlMap(childUrlMap)
        .putAllChildDataCenterControllerD2Map(childD2Map)
        .setD2ServiceName("VeniceController")
        .build();
    doReturn(response).when(requestHandler).listChildClusters(any(ListChildClustersGrpcRequest.class));

    ListChildClustersGrpcRequest request =
        ListChildClustersGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    ListChildClustersGrpcResponse actualResponse = blockingStub.listChildClusters(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getChildDataCenterControllerUrlMapCount(), 2, "Should have 2 URL mappings");
    assertEquals(
        actualResponse.getChildDataCenterControllerUrlMapMap().get("dc1"),
        "http://dc1-controller:8080",
        "DC1 URL should match");
    assertEquals(actualResponse.getChildDataCenterControllerD2MapCount(), 2, "Should have 2 D2 mappings");
    assertEquals(actualResponse.getD2ServiceName(), "VeniceController", "D2 service name should match");
  }

  @Test
  public void testListChildClustersReturnsEmptyForChildController() {
    // Child controller returns empty maps
    ListChildClustersGrpcResponse response =
        ListChildClustersGrpcResponse.newBuilder().setClusterName(TEST_CLUSTER).build();
    doReturn(response).when(requestHandler).listChildClusters(any(ListChildClustersGrpcRequest.class));

    ListChildClustersGrpcRequest request =
        ListChildClustersGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    ListChildClustersGrpcResponse actualResponse = blockingStub.listChildClusters(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getChildDataCenterControllerUrlMapCount(), 0, "Should have no URL mappings");
    assertEquals(actualResponse.getChildDataCenterControllerD2MapCount(), 0, "Should have no D2 mappings");
    assertFalse(actualResponse.hasD2ServiceName(), "Should have no D2 service name");
  }

  @Test
  public void testListChildClustersReturnsErrorResponse() {
    doThrow(new VeniceException("Failed to list child clusters")).when(requestHandler)
        .listChildClusters(any(ListChildClustersGrpcRequest.class));

    ListChildClustersGrpcRequest request =
        ListChildClustersGrpcRequest.newBuilder().setClusterName(TEST_CLUSTER).build();
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.listChildClusters(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("Failed to list child clusters"));
  }

  @Test
  public void testListChildClustersReturnsBadRequestForMissingClusterName() {
    doThrow(new IllegalArgumentException("Cluster name is required")).when(requestHandler)
        .listChildClusters(any(ListChildClustersGrpcRequest.class));

    ListChildClustersGrpcRequest request = ListChildClustersGrpcRequest.newBuilder().build();
    StatusRuntimeException e =
        expectThrows(StatusRuntimeException.class, () -> blockingStub.listChildClusters(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Cluster name is required"));
  }
}
