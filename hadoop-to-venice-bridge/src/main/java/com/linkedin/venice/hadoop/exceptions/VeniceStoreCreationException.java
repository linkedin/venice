package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;


/**
 * Customized exception for receiving invalid {@link VersionCreationResponse}
 * in {@link VenicePushJob}
 */
public class VeniceStoreCreationException extends VeniceException {
  private String storeName;

  public VeniceStoreCreationException(String storeName, String message) {
    super(message);
    this.storeName = storeName;
  }

  public VeniceStoreCreationException(String storeName, String message, Throwable throwable) {
    super(message, throwable);
    this.storeName = storeName;
  }

  public String getStoreName() {
    return this.storeName;
  }

}
