package com.linkedin.venice.grpc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.security.SSLFactory;
import io.grpc.BindableService;
import io.grpc.ServerCredentials;
import io.grpc.ServerInterceptor;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.testng.annotations.Test;


public class VeniceGrpcServerConfigTest {
  @Test
  public void testBuilderWithAllFieldsSet() {
    ServerCredentials credentials = mock(ServerCredentials.class);
    BindableService service = mock(BindableService.class);
    ServerInterceptor interceptor = mock(ServerInterceptor.class);
    SSLFactory sslFactory = mock(SSLFactory.class);
    Executor executor = Executors.newSingleThreadExecutor();

    VeniceGrpcServerConfig config = new VeniceGrpcServerConfig.Builder().setPort(8080)
        .setCredentials(credentials)
        .setService(service)
        .setInterceptor(interceptor)
        .setSslFactory(sslFactory)
        .setExecutor(executor)
        .build();

    assertEquals(config.getPort(), 8080);
    assertEquals(config.getCredentials(), credentials);
    assertEquals(config.getService(), service);
    assertEquals(config.getInterceptors(), Collections.singletonList(interceptor));
    assertEquals(config.getSslFactory(), sslFactory);
    assertEquals(config.getExecutor(), executor);
  }

  @Test
  public void testBuilderWithDefaultInterceptors() {
    BindableService service = mock(BindableService.class);

    VeniceGrpcServerConfig config =
        new VeniceGrpcServerConfig.Builder().setPort(8080).setService(service).setNumThreads(2).build();

    assertTrue(config.getInterceptors().isEmpty());
    assertNotNull(config.getExecutor());
  }

  @Test
  public void testBuilderWithDefaultExecutor() {
    BindableService service = mock(BindableService.class);

    VeniceGrpcServerConfig config =
        new VeniceGrpcServerConfig.Builder().setPort(8080).setService(service).setNumThreads(4).build();

    assertNotNull(config.getExecutor());
    assertEquals(((ThreadPoolExecutor) config.getExecutor()).getCorePoolSize(), 4);
  }

  @Test
  public void testToStringMethod() {
    BindableService service = mock(BindableService.class);
    when(service.toString()).thenReturn("MockService");

    VeniceGrpcServerConfig config =
        new VeniceGrpcServerConfig.Builder().setPort(9090).setService(service).setNumThreads(2).build();

    String expectedString = "VeniceGrpcServerConfig{port=9090, service=MockService}";
    assertEquals(config.toString(), expectedString);
  }

  @Test
  public void testBuilderValidationWithMissingPort() {
    BindableService service = mock(BindableService.class);

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
      new VeniceGrpcServerConfig.Builder().setService(service).setNumThreads(2).build();
    });

    assertEquals(exception.getMessage(), "Port value is required to create the gRPC server but was not provided.");
  }

  @Test
  public void testBuilderValidationWithMissingService() {
    IllegalArgumentException exception = expectThrows(
        IllegalArgumentException.class,
        () -> new VeniceGrpcServerConfig.Builder().setPort(8080).setNumThreads(2).build());

    assertEquals(exception.getMessage(), "A non-null gRPC service instance is required to create the server.");
  }

  @Test
  public void testBuilderValidationWithInvalidThreadsAndExecutor() {
    BindableService service = mock(BindableService.class);

    IllegalArgumentException exception = expectThrows(
        IllegalArgumentException.class,
        () -> new VeniceGrpcServerConfig.Builder().setPort(8080).setService(service).build());

    assertEquals(
        exception.getMessage(),
        "gRPC server creation requires a valid number of threads (numThreads > 0) or a non-null executor.");
  }
}
