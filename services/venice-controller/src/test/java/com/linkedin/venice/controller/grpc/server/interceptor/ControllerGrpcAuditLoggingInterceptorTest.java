package com.linkedin.venice.controller.grpc.server.interceptor;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.controller.grpc.ControllerGrpcConstants;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ControllerGrpcAuditLoggingInterceptorTest {
  private ControllerGrpcAuditLoggingInterceptor interceptor;
  private ServerCall<String, String> mockServerCall;
  private Metadata mockMetadata;
  private ServerCallHandler<String, String> mockHandler;

  @BeforeMethod
  public void setUp() {
    interceptor = new ControllerGrpcAuditLoggingInterceptor();
    mockServerCall = mock(ServerCall.class);
    mockMetadata = new Metadata();
    mockHandler = mock(ServerCallHandler.class);

    // Create a type-safe MethodDescriptor with mock marshallers
    MethodDescriptor.Marshaller<String> stringMarshaller = mock(Marshaller.class);

    MethodDescriptor<String, String> mockMethodDescriptor = MethodDescriptor.<String, String>newBuilder()
        .setFullMethodName("f.q.m.TestService/TestMethod")
        .setType(MethodDescriptor.MethodType.UNARY)
        .setRequestMarshaller(stringMarshaller)
        .setResponseMarshaller(stringMarshaller)
        .build();

    when(mockServerCall.getMethodDescriptor()).thenReturn(mockMethodDescriptor);
  }

  @Test
  public void testInterceptCallWithValidAddressesAndMetadata() {
    // Set up attributes and metadata
    SocketAddress serverAddr = new InetSocketAddress("127.0.0.1", 8080);
    SocketAddress clientAddr = new InetSocketAddress("192.168.1.1", 12345);
    Attributes attributes = Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, serverAddr)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, clientAddr)
        .build();
    when(mockServerCall.getAttributes()).thenReturn(attributes);
    mockMetadata.put(ControllerGrpcConstants.CLUSTER_NAME_METADATA_KEY, "TestCluster");
    mockMetadata.put(ControllerGrpcConstants.STORE_NAME_METADATA_KEY, "TestStore");

    // Invoke the interceptor
    interceptor.interceptCall(mockServerCall, mockMetadata, mockHandler);

    // Verify logging behavior
    ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);
    verify(mockHandler, times(1)).startCall(any(), metadataCaptor.capture());
    assertEquals(metadataCaptor.getValue(), mockMetadata);
  }

  @Test
  public void testInterceptCallWithMissingMetadata() {
    // Set up attributes without metadata
    SocketAddress serverAddr = new InetSocketAddress("127.0.0.1", 8080);
    SocketAddress clientAddr = new InetSocketAddress("192.168.1.1", 12345);
    Attributes attributes = Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, serverAddr)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, clientAddr)
        .build();
    when(mockServerCall.getAttributes()).thenReturn(attributes);

    // Invoke the interceptor
    interceptor.interceptCall(mockServerCall, mockMetadata, mockHandler);

    // Ensure no exceptions occur and verify log format
    verify(mockHandler, times(1)).startCall(any(), any());
  }

  @Test
  public void testResponseLogging() {
    // Set up attributes and metadata
    SocketAddress serverAddr = new InetSocketAddress("127.0.0.1", 8080);
    SocketAddress clientAddr = new InetSocketAddress("192.168.1.1", 12345);
    Attributes attributes = Attributes.newBuilder()
        .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, serverAddr)
        .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, clientAddr)
        .build();
    when(mockServerCall.getAttributes()).thenReturn(attributes);

    mockMetadata.put(ControllerGrpcConstants.CLUSTER_NAME_METADATA_KEY, "TestCluster");
    mockMetadata.put(ControllerGrpcConstants.STORE_NAME_METADATA_KEY, "TestStore");

    // Mock ServerCallHandler to return a dummy Listener
    ServerCall.Listener<String> mockListener = mock(ServerCall.Listener.class);
    when(mockHandler.startCall(any(), any())).thenReturn(mockListener);

    // Capture and verify the listener
    ServerCall.Listener<String> returnedListener = interceptor.interceptCall(mockServerCall, mockMetadata, mockHandler);
    assertNotNull(returnedListener, "The returned listener should not be null.");
    assertEquals(returnedListener, mockListener, "The returned listener should match the mock listener.");

    // Verify response logging behavior
    ArgumentCaptor<ServerCall<String, String>> callCaptor = ArgumentCaptor.forClass(ServerCall.class);
    verify(mockHandler, times(1)).startCall(callCaptor.capture(), eq(mockMetadata));

    ServerCall<String, String> capturedCall = callCaptor.getValue();
    assertNotNull(capturedCall, "Captured ServerCall should not be null.");

    capturedCall.close(Status.OK, new Metadata());
    verify(mockServerCall, times(1)).close(eq(Status.OK), any());
  }
}
