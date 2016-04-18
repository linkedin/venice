package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;

/**
 * Customized exception for receiving invalid {@link com.linkedin.venice.controllerapi.StoreCreationResponse}
 * in {@link com.linkedin.venice.hadoop.KafkaPushJob}
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
