package com.linkedin.venice.routerapi;

import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;


/**
 * Hybrid store quota status response for a resource; this is a response
 * that will be returned by Router.
 */
public class HybridStoreQuotaStatusResponse extends ControllerResponse {
  private HybridStoreQuotaStatus quotaStatus = HybridStoreQuotaStatus.UNKNOWN;

  public HybridStoreQuotaStatus getQuotaStatus() {
    return quotaStatus;
  }

  public void setQuotaStatus(HybridStoreQuotaStatus quotaStatus) {
    this.quotaStatus = quotaStatus;
  }
}
