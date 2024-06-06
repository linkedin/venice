package com.linkedin.venice.exceptions;

/**
 * Class for all Venice exceptions that are triggered by store partitions not being found.
 */
public class StorePartitionNotFoundException extends VeniceNoStoreException {
  public StorePartitionNotFoundException(String storeName, int partition) {
    super("Could not find store partition! Store: " + storeName + "; partition: " + partition + ".");
  }
}
