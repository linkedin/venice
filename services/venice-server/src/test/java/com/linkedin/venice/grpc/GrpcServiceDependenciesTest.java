package com.linkedin.venice.grpc;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.listener.NoOpReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class GrpcServiceDependenciesTest {
  @Test
  public void testBuilderValidation() {
    DiskHealthCheckService diskHealthCheckService = Mockito.mock(DiskHealthCheckService.class);
    StorageReadRequestHandler storageReadRequestHandler = Mockito.mock(StorageReadRequestHandler.class);
    NoOpReadQuotaEnforcementHandler quotaEnforcementHandler = Mockito.mock(NoOpReadQuotaEnforcementHandler.class);
    AggServerHttpRequestStats singleGetStats = Mockito.mock(AggServerHttpRequestStats.class);
    AggServerHttpRequestStats multiGetStats = Mockito.mock(AggServerHttpRequestStats.class);
    AggServerHttpRequestStats computeStats = Mockito.mock(AggServerHttpRequestStats.class);
    GrpcReplyProcessor grpcReplyProcessor = Mockito.mock(GrpcReplyProcessor.class);

    // Test with all fields set
    GrpcServiceDependencies dependencies =
        new GrpcServiceDependencies.Builder().setDiskHealthCheckService(diskHealthCheckService)
            .setStorageReadRequestHandler(storageReadRequestHandler)
            .setQuotaEnforcementHandler(quotaEnforcementHandler)
            .setSingleGetStats(singleGetStats)
            .setMultiGetStats(multiGetStats)
            .setComputeStats(computeStats)
            .setGrpcReplyProcessor(grpcReplyProcessor)
            .build();

    assertNotNull(dependencies);
    assertSame(dependencies.getDiskHealthCheckService(), diskHealthCheckService);
    assertSame(dependencies.getStorageReadRequestHandler(), storageReadRequestHandler);
    assertSame(dependencies.getQuotaEnforcementHandler(), quotaEnforcementHandler);
    assertSame(dependencies.getSingleGetStats(), singleGetStats);
    assertSame(dependencies.getMultiGetStats(), multiGetStats);
    assertSame(dependencies.getComputeStats(), computeStats);
    assertSame(dependencies.getGrpcReplyProcessor(), grpcReplyProcessor);
  }

  @Test
  public void testBuilderValidationWithMissingFields() {
    assertThrows(
        NullPointerException.class,
        () -> new GrpcServiceDependencies.Builder().setDiskHealthCheckService(null).build());
  }

  @Test
  public void testBuilderValidationWithDefaultValues() {
    DiskHealthCheckService diskHealthCheckService = Mockito.mock(DiskHealthCheckService.class);
    StorageReadRequestHandler storageReadRequestHandler = Mockito.mock(StorageReadRequestHandler.class);
    AggServerHttpRequestStats singleGetStats = Mockito.mock(AggServerHttpRequestStats.class);
    AggServerHttpRequestStats multiGetStats = Mockito.mock(AggServerHttpRequestStats.class);
    AggServerHttpRequestStats computeStats = Mockito.mock(AggServerHttpRequestStats.class);

    GrpcServiceDependencies dependencies =
        new GrpcServiceDependencies.Builder().setDiskHealthCheckService(diskHealthCheckService)
            .setStorageReadRequestHandler(storageReadRequestHandler)
            .setSingleGetStats(singleGetStats)
            .setMultiGetStats(multiGetStats)
            .setComputeStats(computeStats)
            .build();

    assertNotNull(dependencies);
    assertTrue(dependencies.getQuotaEnforcementHandler() instanceof NoOpReadQuotaEnforcementHandler);
    assertNotNull(dependencies.getGrpcReplyProcessor());
    assertSame(dependencies.getDiskHealthCheckService(), diskHealthCheckService);
    assertSame(dependencies.getStorageReadRequestHandler(), storageReadRequestHandler);
    assertSame(dependencies.getSingleGetStats(), singleGetStats);
    assertSame(dependencies.getMultiGetStats(), multiGetStats);
    assertSame(dependencies.getComputeStats(), computeStats);
  }
}
