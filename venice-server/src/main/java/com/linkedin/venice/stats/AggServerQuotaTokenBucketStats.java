package com.linkedin.venice.stats;

import com.linkedin.venice.listener.StorageQuotaEnforcementHandler;
import io.tehuti.metrics.MetricsRepository;


public class AggServerQuotaTokenBucketStats extends AbstractVeniceAggStats<ServerQuotaTokenBucketStats>{
  public AggServerQuotaTokenBucketStats(MetricsRepository metricsRepository, StorageQuotaEnforcementHandler quotaEnforcer) {
    super(metricsRepository,
        (metrics, storeName) -> new ServerQuotaTokenBucketStats(metrics, storeName,
            () -> quotaEnforcer.getBucketForStore(storeName))
    );
  }
}
