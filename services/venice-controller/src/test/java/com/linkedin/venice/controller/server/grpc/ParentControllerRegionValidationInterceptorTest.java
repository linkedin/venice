package com.linkedin.venice.controller.server.grpc;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ParentControllerRegionState;
import com.linkedin.venice.protocols.controller.VeniceControllerGrpcServiceGrpc;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ParentControllerRegionValidationInterceptorTest {
  private ParentControllerRegionValidationInterceptor interceptor;
  private Admin admin;
  private ServerCall<Object, Object> call;
  private Metadata headers;
  private ServerCallHandler<Object, Object> next;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    call = mock(ServerCall.class, RETURNS_DEEP_STUBS);
    headers = new Metadata();
    next = mock(ServerCallHandler.class);
    interceptor = new ParentControllerRegionValidationInterceptor(admin);
  }

  @Test
  public void testActiveParentControllerPasses() {
    when(admin.isParent()).thenReturn(true);
    when(admin.getParentControllerRegionState()).thenReturn(ParentControllerRegionState.ACTIVE);

    interceptor.interceptCall(call, headers, next);

    verify(next, times(1)).startCall(call, headers);
    verify(call, never()).close(any(), any());
  }

  @Test
  public void testInactiveParentControllerRejectsRequest() {
    when(admin.isParent()).thenReturn(true);
    when(admin.getParentControllerRegionState()).thenReturn(ParentControllerRegionState.PASSIVE);

    MethodDescriptor methodDescriptor = VeniceControllerGrpcServiceGrpc.getGetLeaderControllerMethod();
    when(call.getMethodDescriptor()).thenReturn(methodDescriptor);
    when(call.getAttributes()).thenReturn(Attributes.EMPTY);

    interceptor.interceptCall(call, headers, next);

    verify(call, times(1)).close(any(io.grpc.Status.class), any(Metadata.class));
    verify(next, never()).startCall(call, headers);
  }

  @Test
  public void testNonParentControllerPasses() {
    when(admin.isParent()).thenReturn(false);

    interceptor.interceptCall(call, headers, next);

    verify(next, times(1)).startCall(call, headers);
    verify(call, never()).close(any(), any());
  }

  @Test
  public void testErrorMessageAndCodeOnRejection() {
    when(admin.isParent()).thenReturn(true);
    when(admin.getParentControllerRegionState()).thenReturn(ParentControllerRegionState.PASSIVE);
    MethodDescriptor methodDescriptor = VeniceControllerGrpcServiceGrpc.getGetLeaderControllerMethod();
    when(call.getMethodDescriptor()).thenReturn(methodDescriptor);
    when(call.getAttributes()).thenReturn(Attributes.EMPTY);

    interceptor.interceptCall(call, headers, next);

    ArgumentCaptor<io.grpc.Status> statusCaptor = ArgumentCaptor.forClass(io.grpc.Status.class);
    ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(call, times(1)).close(statusCaptor.capture(), metadataCaptor.capture());
    verify(next, never()).startCall(call, headers);

    io.grpc.Status status = statusCaptor.getValue();
    assertTrue(status.getDescription().contains("Parent controller is not active"));
    assertEquals(status.getCode(), io.grpc.Status.FAILED_PRECONDITION.getCode());
  }
}
