package com.linkedin.venice.router.stats;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import io.tehuti.metrics.MetricsRepository;

public class AggRouterHttpRequestStats extends AbstractVeniceAggStats<RouterHttpRequestStats>{
  public AggRouterHttpRequestStats(MetricsRepository metricsRepository, RequestType requestType) {
    super(metricsRepository,
        (metricsRepo, storeName) -> new RouterHttpRequestStats(metricsRepo, storeName, requestType));
  }

  public void recordRequest(String storeName) {
    totalStats.recordRequest();
    getStoreStats(storeName).recordRequest();
  }

  public void recordHealthyRequest(String storeName) {
    totalStats.recordHealthyRequest();
    getStoreStats(storeName).recordHealthyRequest();
  }

  public void recordUnhealthyRequest(String storeName) {
    totalStats.recordUnhealthyRequest();
    getStoreStats(storeName).recordUnhealthyRequest();
  }
  public void recordUnhealthyRequest() {
    totalStats.recordUnhealthyRequest();
  }

  public void recordThrottledRequest(String storeName){
    totalStats.recordThrottledRequest();
    getStoreStats(storeName).recordThrottledRequest();
  }

  public void recordBadRequest(String storeName) {
    totalStats.recordBadRequest();
    getStoreStats(storeName).recordBadRequest();
  }

  public void recordBadRequest() {
    totalStats.recordBadRequest();
  }

  public void recordFanoutRequestCount(String storeName, int count) {
    totalStats.recordFanoutRequestCount(count);
    getStoreStats(storeName).recordFanoutRequestCount(count);
  }

  public void recordLatency(String storeName, double latency) {
    totalStats.recordLatency(latency);
    getStoreStats(storeName).recordLatency(latency);
  }

  public void recordResponseWaitingTime(String storeName, double waitingTime) {
    totalStats.recordResponseWaitingTime(waitingTime);
    getStoreStats(storeName).recordResponseWaitingTime(waitingTime);
  }

  public void recordRequestSize(String storeName, double keySize) {
    totalStats.recordRequestSize(keySize);
    getStoreStats(storeName).recordRequestSize(keySize);
  }

  public void recordResponseSize(String storeName, double valueSize) {
    totalStats.recordResponseSize(valueSize);
    getStoreStats(storeName).recordResponseSize(valueSize);
  }

  public void recordQuota(String storeName, double quota) {
    getStoreStats(storeName).recordQuota(quota);
  }

  public void recordTotalQuota(double totalQuota) {
    totalStats.recordQuota(totalQuota);
  }

  public void recordFindUnhealthyHostRequest(String storeName) {
    totalStats.recordFindUnhealthyHostRequest();
    getStoreStats(storeName).recordFindUnhealthyHostRequest();
  }
}
