package com.linkedin.venice.listener;

import com.linkedin.venice.listener.request.RouterRequest;


/**
 * Interface for enforcing quota on requests.
 */
public interface QuotaEnforcementHandler {
  QuotaEnforcementResult enforceQuota(RouterRequest request);

  enum QuotaEnforcementResult {
    ALLOWED, // request is allowed
    REJECTED, // too many requests (store level quota enforcement)
    OVER_CAPACITY, // server over capacity (server level quota enforcement)
    BAD_REQUEST, // bad request
  }
}
