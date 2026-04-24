package com.linkedin.venice.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tracks progress of a bulk recovery operation for a datacenter.
 */
public class RecoveryProgress {
  private final String datacenterName;
  private final AtomicInteger totalStores = new AtomicInteger(0);
  private final AtomicInteger recoveredStores = new AtomicInteger(0);
  private final AtomicInteger failedStores = new AtomicInteger(0);
  private final AtomicInteger versionsTransitioned = new AtomicInteger(0);
  private final List<StoreVersionPair> initiatedStores = Collections.synchronizedList(new ArrayList<>());
  private volatile boolean complete = false;

  public RecoveryProgress(String datacenterName) {
    this.datacenterName = datacenterName;
  }

  public String getDatacenterName() {
    return datacenterName;
  }

  public int getTotalStores() {
    return totalStores.get();
  }

  public void setTotalStores(int total) {
    totalStores.set(total);
  }

  public int getRecoveredStores() {
    return recoveredStores.get();
  }

  public void incrementRecovered() {
    recoveredStores.incrementAndGet();
  }

  public int getFailedStores() {
    return failedStores.get();
  }

  public void incrementFailed() {
    failedStores.incrementAndGet();
  }

  public int getVersionsTransitioned() {
    return versionsTransitioned.get();
  }

  public void incrementVersionsTransitioned() {
    versionsTransitioned.incrementAndGet();
  }

  public void addInitiatedStore(StoreVersionPair sv) {
    initiatedStores.add(sv);
  }

  public List<StoreVersionPair> getInitiatedStores() {
    return initiatedStores;
  }

  public boolean isComplete() {
    return complete;
  }

  public void markComplete() {
    complete = true;
  }

  public double getProgressFraction() {
    int total = totalStores.get();
    if (total == 0) {
      return complete ? 1.0 : 0.0;
    }
    return (double) (recoveredStores.get() + failedStores.get()) / total;
  }

  static class StoreVersionPair {
    final String storeName;
    final int version;

    StoreVersionPair(String storeName, int version) {
      this.storeName = storeName;
      this.version = version;
    }
  }
}
