package com.linkedin.venice.controller.grpc.server;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.grpc.GrpcRequestResponseConverter;
import com.linkedin.venice.controller.grpc.server.ControllerGrpcServerUtils.GrpcRequestHandler;
import com.linkedin.venice.exceptions.VeniceUnauthorizedAccessException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.ControllerGrpcErrorType;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcRequest;
import com.linkedin.venice.protocols.controller.LeaderControllerGrpcResponse;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcErrorInfo;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ControllerGrpcServerUtilsTest {
  private static final String CLUSTER_NAME = "TestCluster";
  private static final String STORE_NAME = "TestStore";

  private MethodDescriptor<LeaderControllerGrpcRequest, LeaderControllerGrpcResponse> methodDescriptor;
  private StreamObserver<String> mockResponseObserver;
  private ClusterStoreGrpcInfo storeGrpcInfo;

  @BeforeMethod
  public void setUp() {
    methodDescriptor = VeniceControllerGrpcServiceGrpc.getGetLeaderControllerMethod();
    mockResponseObserver = mock(StreamObserver.class);
    storeGrpcInfo = ClusterStoreGrpcInfo.newBuilder().setClusterName(CLUSTER_NAME).setStoreName(STORE_NAME).build();
  }

  @Test
  public void testHandleRequestSuccess() {
    GrpcRequestHandler<String> handler = () -> "Success";

    ControllerGrpcServerUtils.handleRequest(methodDescriptor, handler, mockResponseObserver, storeGrpcInfo);

    verify(mockResponseObserver, times(1)).onNext("Success");
    verify(mockResponseObserver, times(1)).onCompleted();
    verify(mockResponseObserver, never()).onError(any());
  }

  @Test
  public void testHandleRequestInvalidArgument() {
    GrpcRequestHandler<String> handler = () -> {
      throw new IllegalArgumentException("Invalid argument");
    };

    ControllerGrpcServerUtils.handleRequest(methodDescriptor, handler, mockResponseObserver, storeGrpcInfo);

    ArgumentCaptor<StatusRuntimeException> statusCaptor = ArgumentCaptor.forClass(StatusRuntimeException.class);
    verify(mockResponseObserver, never()).onNext(any());
    verify(mockResponseObserver, never()).onCompleted();
    verify(mockResponseObserver, times(1)).onError(statusCaptor.capture());
    StatusRuntimeException exception = statusCaptor.getValue();
    assertEquals(exception.getStatus().getCode(), Status.INVALID_ARGUMENT.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(exception);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.BAD_REQUEST);
    assertTrue(errorInfo.getErrorMessage().contains("Invalid argument"));
  }

  @Test
  public void testHandleRequestUnauthorizedAccess() {
    GrpcRequestHandler<String> handler = () -> {
      throw new VeniceUnauthorizedAccessException("Unauthorized access");
    };

    ControllerGrpcServerUtils.handleRequest(methodDescriptor, handler, mockResponseObserver, storeGrpcInfo);

    ArgumentCaptor<StatusRuntimeException> statusCaptor = ArgumentCaptor.forClass(StatusRuntimeException.class);
    verify(mockResponseObserver, never()).onNext(any());
    verify(mockResponseObserver, never()).onCompleted();
    verify(mockResponseObserver, times(1)).onError(statusCaptor.capture());
    StatusRuntimeException exception = statusCaptor.getValue();
    assertEquals(exception.getStatus().getCode(), Status.PERMISSION_DENIED.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(exception);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.UNAUTHORIZED);
    assertTrue(errorInfo.getErrorMessage().contains("Unauthorized access"));
  }

  @Test
  public void testHandleRequestGeneralException() {
    GrpcRequestHandler<String> handler = () -> {
      throw new RuntimeException("General error");
    };

    ControllerGrpcServerUtils.handleRequest(methodDescriptor, handler, mockResponseObserver, storeGrpcInfo);

    ArgumentCaptor<StatusRuntimeException> statusCaptor = ArgumentCaptor.forClass(StatusRuntimeException.class);
    verify(mockResponseObserver, never()).onNext(any());
    verify(mockResponseObserver, never()).onCompleted();
    verify(mockResponseObserver, times(1)).onError(statusCaptor.capture());
    StatusRuntimeException exception = statusCaptor.getValue();
    assertEquals(exception.getStatus().getCode(), Status.INTERNAL.getCode());
    VeniceControllerGrpcErrorInfo errorInfo = GrpcRequestResponseConverter.parseControllerGrpcError(exception);
    assertEquals(errorInfo.getErrorType(), ControllerGrpcErrorType.GENERAL_ERROR);
    assertTrue(errorInfo.getErrorMessage().contains("General error"));
  }
}
