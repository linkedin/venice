package com.linkedin.venice.controller.grpc.server;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.server.SchemaRequestHandler;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetKeySchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.protocols.controller.SchemaGrpcServiceGrpc;
import com.linkedin.venice.protocols.controller.SchemaGrpcServiceGrpc.SchemaGrpcServiceBlockingStub;
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


public class SchemaGrpcServiceImplTest {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_STORE = "test-store";
  private static final String VALUE_SCHEMA = "string";

  private Server grpcServer;
  private ManagedChannel grpcChannel;
  private SchemaRequestHandler schemaRequestHandler;
  private SchemaGrpcServiceBlockingStub blockingStub;

  @BeforeMethod
  public void setUp() throws Exception {
    schemaRequestHandler = mock(SchemaRequestHandler.class);

    // Create a unique server name for the in-process server
    String serverName = InProcessServerBuilder.generateName();

    // Start the gRPC server in-process
    grpcServer = InProcessServerBuilder.forName(serverName)
        .directExecutor()
        .addService(new SchemaGrpcServiceImpl(schemaRequestHandler))
        .build()
        .start();

    // Create a channel to communicate with the server
    grpcChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    // Create a blocking stub to make calls to the server
    blockingStub = SchemaGrpcServiceGrpc.newBlockingStub(grpcChannel);
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
  public void testGetValueSchemaReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetValueSchemaGrpcRequest request =
        GetValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).setSchemaId(1).build();
    GetValueSchemaGrpcResponse expectedResponse = GetValueSchemaGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setSchemaId(1)
        .setSchemaStr(VALUE_SCHEMA)
        .build();
    when(schemaRequestHandler.getValueSchema(any(GetValueSchemaGrpcRequest.class))).thenReturn(expectedResponse);

    GetValueSchemaGrpcResponse actualResponse = blockingStub.getValueSchema(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getStoreInfo().getClusterName(), TEST_CLUSTER, "Cluster name should match");
    assertEquals(actualResponse.getStoreInfo().getStoreName(), TEST_STORE, "Store name should match");
    assertEquals(actualResponse.getSchemaId(), 1, "Schema ID should match");
    assertEquals(actualResponse.getSchemaStr(), VALUE_SCHEMA, "Schema string should match");
  }

  @Test
  public void testGetValueSchemaReturnsErrorWhenSchemaNotFound() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetValueSchemaGrpcRequest request =
        GetValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).setSchemaId(99).build();
    when(schemaRequestHandler.getValueSchema(any(GetValueSchemaGrpcRequest.class)))
        .thenThrow(new IllegalArgumentException("Value schema for schema id: 99 of store: test-store doesn't exist"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getValueSchema(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Value schema for schema id: 99"));
  }

  @Test
  public void testGetValueSchemaReturnsErrorForInvalidInput() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName("").setStoreName(TEST_STORE).build();
    GetValueSchemaGrpcRequest request =
        GetValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).setSchemaId(1).build();
    when(schemaRequestHandler.getValueSchema(any(GetValueSchemaGrpcRequest.class)))
        .thenThrow(new IllegalArgumentException("Cluster name is mandatory parameter"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getValueSchema(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Cluster name is mandatory parameter"));
  }

  @Test
  public void testGetValueSchemaReturnsGeneralError() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetValueSchemaGrpcRequest request =
        GetValueSchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).setSchemaId(1).build();
    when(schemaRequestHandler.getValueSchema(any(GetValueSchemaGrpcRequest.class)))
        .thenThrow(new VeniceException("Internal error fetching schema"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getValueSchema(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("Internal error fetching schema"));
  }

  @Test
  public void testGetKeySchemaReturnsSuccessfulResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetKeySchemaGrpcRequest request = GetKeySchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    GetKeySchemaGrpcResponse response =
        GetKeySchemaGrpcResponse.newBuilder().setStoreInfo(storeInfo).setSchemaId(1).setSchemaStr("\"string\"").build();
    when(schemaRequestHandler.getKeySchema(any(GetKeySchemaGrpcRequest.class))).thenReturn(response);

    GetKeySchemaGrpcResponse actualResponse = blockingStub.getKeySchema(request);

    assertNotNull(actualResponse, "Response should not be null");
    assertEquals(actualResponse.getStoreInfo(), storeInfo, "Store info should match");
    assertEquals(actualResponse.getSchemaId(), 1, "Schema ID should match");
    assertEquals(actualResponse.getSchemaStr(), "\"string\"", "Schema string should match");
  }

  @Test
  public void testGetKeySchemaReturnsErrorResponse() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetKeySchemaGrpcRequest request = GetKeySchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(schemaRequestHandler.getKeySchema(any(GetKeySchemaGrpcRequest.class)))
        .thenThrow(new VeniceException("Key schema doesn't exist for store: " + TEST_STORE));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getKeySchema(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("Key schema doesn't exist for store"));
  }

  @Test
  public void testGetKeySchemaReturnsBadRequestForInvalidArgument() {
    ClusterStoreGrpcInfo storeInfo =
        ClusterStoreGrpcInfo.newBuilder().setClusterName(TEST_CLUSTER).setStoreName(TEST_STORE).build();
    GetKeySchemaGrpcRequest request = GetKeySchemaGrpcRequest.newBuilder().setStoreInfo(storeInfo).build();
    when(schemaRequestHandler.getKeySchema(any(GetKeySchemaGrpcRequest.class)))
        .thenThrow(new IllegalArgumentException("Cluster name is mandatory parameter"));

    StatusRuntimeException e = expectThrows(StatusRuntimeException.class, () -> blockingStub.getKeySchema(request));

    assertNotNull(e.getStatus(), "Status should not be null");
    assertEquals(e.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(e);
    assertNotNull(errorInfo, "Error info should not be null");
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Cluster name is mandatory parameter"));
  }
}
