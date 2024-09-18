package com.linkedin.venice.grpc;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.listener.NoOpReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.QuotaEnforcementHandler;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.stats.AggServerHttpRequestStats;
import java.util.Objects;


public class GrpcServiceDependencies {
  private final DiskHealthCheckService diskHealthCheckService;
  private final StorageReadRequestHandler storageReadRequestHandler;
  private final QuotaEnforcementHandler quotaEnforcementHandler;
  private final AggServerHttpRequestStats singleGetStats;
  private final AggServerHttpRequestStats multiGetStats;
  private final AggServerHttpRequestStats computeStats;
  private final GrpcReplyProcessor grpcReplyProcessor;

  private GrpcServiceDependencies(Builder builder) {
    this.diskHealthCheckService = builder.diskHealthCheckService;
    this.storageReadRequestHandler = builder.storageReadRequestHandler;
    this.quotaEnforcementHandler = builder.quotaEnforcementHandler;
    this.singleGetStats = builder.singleGetStats;
    this.multiGetStats = builder.multiGetStats;
    this.computeStats = builder.computeStats;
    this.grpcReplyProcessor = builder.grpcReplyProcessor;
  }

  public DiskHealthCheckService getDiskHealthCheckService() {
    return diskHealthCheckService;
  }

  public StorageReadRequestHandler getStorageReadRequestHandler() {
    return storageReadRequestHandler;
  }

  public QuotaEnforcementHandler getQuotaEnforcementHandler() {
    return quotaEnforcementHandler;
  }

  public AggServerHttpRequestStats getSingleGetStats() {
    return singleGetStats;
  }

  public AggServerHttpRequestStats getMultiGetStats() {
    return multiGetStats;
  }

  public AggServerHttpRequestStats getComputeStats() {
    return computeStats;
  }

  public GrpcReplyProcessor getGrpcReplyProcessor() {
    return grpcReplyProcessor;
  }

  public static class Builder {
    private DiskHealthCheckService diskHealthCheckService;
    private StorageReadRequestHandler storageReadRequestHandler;
    private QuotaEnforcementHandler quotaEnforcementHandler;
    private AggServerHttpRequestStats singleGetStats;
    private AggServerHttpRequestStats multiGetStats;
    private AggServerHttpRequestStats computeStats;
    private GrpcReplyProcessor grpcReplyProcessor;

    public Builder setDiskHealthCheckService(DiskHealthCheckService diskHealthCheckService) {
      this.diskHealthCheckService = diskHealthCheckService;
      return this;
    }

    public Builder setStorageReadRequestHandler(StorageReadRequestHandler storageReadRequestHandler) {
      this.storageReadRequestHandler = storageReadRequestHandler;
      return this;
    }

    public Builder setQuotaEnforcementHandler(QuotaEnforcementHandler quotaEnforcementHandler) {
      this.quotaEnforcementHandler = quotaEnforcementHandler;
      return this;
    }

    public Builder setSingleGetStats(AggServerHttpRequestStats singleGetStats) {
      this.singleGetStats = singleGetStats;
      return this;
    }

    public Builder setMultiGetStats(AggServerHttpRequestStats multiGetStats) {
      this.multiGetStats = multiGetStats;
      return this;
    }

    public Builder setComputeStats(AggServerHttpRequestStats computeStats) {
      this.computeStats = computeStats;
      return this;
    }

    public Builder setGrpcReplyProcessor(GrpcReplyProcessor grpcReplyProcessor) {
      this.grpcReplyProcessor = grpcReplyProcessor;
      return this;
    }

    public GrpcServiceDependencies build() {
      // Validate that all required fields are set
      if (quotaEnforcementHandler == null) {
        quotaEnforcementHandler = NoOpReadQuotaEnforcementHandler.getInstance();
      }
      if (grpcReplyProcessor == null) {
        grpcReplyProcessor = new GrpcReplyProcessor();
      }

      singleGetStats = Objects.requireNonNull(singleGetStats, "singleGetStats cannot be null");
      multiGetStats = Objects.requireNonNull(multiGetStats, "multiGetStats cannot be null");
      computeStats = Objects.requireNonNull(computeStats, "computeStats cannot be null");
      storageReadRequestHandler =
          Objects.requireNonNull(storageReadRequestHandler, "storageReadRequestHandler cannot be null");
      diskHealthCheckService = Objects.requireNonNull(diskHealthCheckService, "diskHealthCheckService cannot be null");

      return new GrpcServiceDependencies(this);
    }
  }
}
