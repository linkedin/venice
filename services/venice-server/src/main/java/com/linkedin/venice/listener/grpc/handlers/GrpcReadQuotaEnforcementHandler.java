package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.venice.listener.ReadQuotaEnforcementHandler;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.AggServerQuotaUsageStats;
import com.linkedin.venice.throttle.TokenBucket;


public class GrpcReadQuotaEnforcementHandler extends VeniceServerGrpcHandler {
  private final ReadQuotaEnforcementHandler readQuota;

  public GrpcReadQuotaEnforcementHandler(ReadQuotaEnforcementHandler readQuotaEnforcementHandler) {
    readQuota = readQuotaEnforcementHandler;
  }

  public void processRequest(GrpcRequestContext ctx) {
    RouterRequest request = ctx.getRouterRequest();
    String storeName = request.getStoreName();
    Store store = readQuota.getStoreRepository().getStore(storeName);

    if (readQuota.checkStoreNull(null, request, ctx, true, store)) {
      invokeNextHandler(ctx);
      return;
    }

    if (readQuota.checkInitAndQuotaEnabled(null, request, store, true)) {
      invokeNextHandler(ctx);
      return;
    }

    int rcu = ReadQuotaEnforcementHandler.getRcu(request);

    TokenBucket tokenBucket = readQuota.getStoreVersionBuckets().get(request.getResourceName());
    if (tokenBucket != null && !request.isRetryRequest() && !tokenBucket.tryConsume(rcu)
        && readQuota.handleTooManyRequests(null, request, ctx, store, rcu, true)) {
      invokeNextHandler(ctx);
      return;
    }

    if (readQuota.isEnforcingAndNoBucketStoreContainsResource(request.getResourceName())) {
      readQuota.handleEnforcingAndNoBucket(request);
    }

    if (readQuota.storageConsumeRcu(rcu) && readQuota.handleServerOverCapacity(null, ctx, storeName, rcu, true)) {
      invokeNextHandler(ctx);
      return;
    }

    AggServerQuotaUsageStats stats = readQuota.getStats();
    stats.recordAllowed(storeName, rcu);
    stats.recordReadQuotaUsage(storeName, readQuota.getStaleUsageRatio());

    invokeNextHandler(ctx);
  }
}
