package com.linkedin.venice.controllerapi.transport;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.Status;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.request.ControllerRequest;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.testng.annotations.Test;


public class GrpcRequestResponseConverterTest {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_STORE = "testStore";

  @Test
  public void testGetClusterStoreGrpcInfoFromResponse() {
    // Test with all fields set
    ControllerResponse response = mock(ControllerResponse.class);
    when(response.getCluster()).thenReturn("testCluster");
    when(response.getName()).thenReturn("testStore");
    ClusterStoreGrpcInfo grpcInfo = GrpcRequestResponseConverter.getClusterStoreGrpcInfo(response);
    assertEquals(grpcInfo.getClusterName(), "testCluster");
    assertEquals(grpcInfo.getStoreName(), "testStore");

    // Test with null fields
    when(response.getCluster()).thenReturn(null);
    when(response.getName()).thenReturn(null);
    grpcInfo = GrpcRequestResponseConverter.getClusterStoreGrpcInfo(response);
    assertEquals(grpcInfo.getClusterName(), "");
    assertEquals(grpcInfo.getStoreName(), "");
  }

  @Test
  public void testGetClusterStoreGrpcInfoFromRequest() {
    // Test with all fields set
    ControllerRequest request = mock(ControllerRequest.class);
    when(request.getClusterName()).thenReturn("testCluster");
    when(request.getStoreName()).thenReturn("testStore");
    ClusterStoreGrpcInfo grpcInfo = GrpcRequestResponseConverter.getClusterStoreGrpcInfo(request);
    assertEquals(grpcInfo.getClusterName(), "testCluster");
    assertEquals(grpcInfo.getStoreName(), "testStore");

    // Test with null fields
    when(request.getClusterName()).thenReturn(null);
    when(request.getStoreName()).thenReturn(null);
    grpcInfo = GrpcRequestResponseConverter.getClusterStoreGrpcInfo(request);
    assertEquals(grpcInfo.getClusterName(), "");
    assertEquals(grpcInfo.getStoreName(), "");
  }

  @Test
  public void testSendErrorResponse() {
    StreamObserver<?> responseObserver = mock(StreamObserver.class);

    Exception e = new Exception("Test error message");
    Code errorCode = Code.INVALID_ARGUMENT;
    ControllerGrpcErrorType errorType = ControllerGrpcErrorType.BAD_REQUEST;

    GrpcRequestResponseConverter.sendErrorResponse(
        io.grpc.Status.Code.INVALID_ARGUMENT,
        ControllerGrpcErrorType.BAD_REQUEST,
        e,
        TEST_CLUSTER,
        TEST_STORE,
        responseObserver);

    verify(responseObserver, times(1)).onError(argThat(statusRuntimeException -> {
      com.google.rpc.Status status = StatusProto.fromThrowable((StatusRuntimeException) statusRuntimeException);

      VeniceControllerGrpcErrorInfo errorInfo = null;
      for (Any detail: status.getDetailsList()) {
        if (detail.is(VeniceControllerGrpcErrorInfo.class)) {
          try {
            errorInfo = detail.unpack(VeniceControllerGrpcErrorInfo.class);
            break;
          } catch (Exception ignored) {
          }
        }
      }

      assertNotNull(errorInfo);
      assertEquals(errorInfo.getErrorType(), errorType);
      assertEquals(errorInfo.getErrorMessage(), "Test error message");
      assertEquals(errorInfo.getClusterName(), "testCluster");
      assertEquals(errorInfo.getStoreName(), "testStore");
      assertEquals(status.getCode(), errorCode.getNumber());

      return true;
    }));
  }

  @Test
  public void testParseControllerGrpcError() {
    // Create a valid VeniceControllerGrpcErrorInfo
    VeniceControllerGrpcErrorInfo errorInfo = VeniceControllerGrpcErrorInfo.newBuilder()
        .setErrorType(ControllerGrpcErrorType.BAD_REQUEST)
        .setErrorMessage("Invalid input")
        .setStatusCode(Code.INVALID_ARGUMENT.getNumber())
        .build();

    // Wrap in a com.google.rpc.Status
    Status rpcStatus =
        Status.newBuilder().setCode(Code.INVALID_ARGUMENT.getNumber()).addDetails(Any.pack(errorInfo)).build();

    // Convert to StatusRuntimeException
    StatusRuntimeException exception = StatusProto.toStatusRuntimeException(rpcStatus);

    // Parse the error
    VeniceControllerGrpcErrorInfo parsedError = GrpcRequestResponseConverter.parseControllerGrpcError(exception);

    // Assert the parsed error matches the original
    assertEquals(parsedError.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertEquals(parsedError.getErrorMessage(), "Invalid input");
    assertEquals(parsedError.getStatusCode(), Code.INVALID_ARGUMENT.getNumber());
  }

  @Test
  public void testParseControllerGrpcErrorWithNoDetails() {
    // Create an exception with no details
    Status rpcStatus = Status.newBuilder().setCode(Code.UNKNOWN.getNumber()).build();
    StatusRuntimeException exception = StatusProto.toStatusRuntimeException(rpcStatus);

    VeniceClientException thrownException = expectThrows(
        VeniceClientException.class,
        () -> GrpcRequestResponseConverter.parseControllerGrpcError(exception));

    assertEquals(thrownException.getMessage(), "An unknown gRPC error occurred. Error code: UNKNOWN");
  }

  @Test
  public void testParseControllerGrpcErrorWithUnpackFailure() {
    // Create a corrupted detail
    Any corruptedDetail = Any.newBuilder()
        .setTypeUrl("type.googleapis.com/" + VeniceControllerGrpcErrorInfo.getDescriptor().getFullName())
        .setValue(com.google.protobuf.ByteString.copyFromUtf8("corrupted data"))
        .build();

    Status rpcStatus =
        Status.newBuilder().setCode(Code.INVALID_ARGUMENT.getNumber()).addDetails(corruptedDetail).build();

    StatusRuntimeException exception = StatusProto.toStatusRuntimeException(rpcStatus);

    VeniceClientException thrownException = expectThrows(VeniceClientException.class, () -> {
      GrpcRequestResponseConverter.parseControllerGrpcError(exception);
    });

    assertTrue(thrownException.getMessage().contains("Failed to unpack error details"));
  }
}
