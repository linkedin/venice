package com.linkedin.venice.controller;

import com.linkedin.venice.exceptions.VeniceException;


/**
 * Thrown when a {@link StoreUpdateHandler} fails while reacting to a durable {@code UPDATE_STORE} admin operation.
 *
 * <p>Handler failures are wrapped in this dedicated type so they cannot be misclassified by the admin consumer's
 * exception classifier. In particular, a handler that throws
 * {@link com.linkedin.venice.exceptions.VeniceNoStoreException} must not be mistaken for the {@code UPDATE_STORE}
 * target being absent, which would trigger the missing-store auto-skip path and permanently drop an operation whose
 * durable update already succeeded. Like other admin-processing exceptions, this exception is retriable: the admin
 * operation stays eligible for retry until the handler succeeds.</p>
 */
public class StoreUpdateCallbackException extends VeniceException {
  public StoreUpdateCallbackException(String clusterName, String storeName, Throwable cause) {
    super(
        "Store update handler failed for cluster: " + clusterName + ", store: " + storeName
            + " after a successful UPDATE_STORE admin operation",
        cause);
  }
}
