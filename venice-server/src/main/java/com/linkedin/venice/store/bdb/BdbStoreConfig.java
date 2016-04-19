package com.linkedin.venice.store.bdb;

import com.google.common.base.Objects;
import com.linkedin.venice.utils.VeniceProperties;

import java.io.Serializable;

/**
 * Store-level BDB configurations.
 */
public class BdbStoreConfig implements Serializable {

  private static final long serialVersionUID = 1;
  private final String storeName;

  private final long memoryFootprintMB;

  private static final String MEMORY_FOOTPRINT_MB = "memory.footprint.mb";

  public BdbStoreConfig(String storeName, VeniceProperties props) {
    this.storeName = storeName;
    this.memoryFootprintMB = props.getLong(MEMORY_FOOTPRINT_MB, 0L);
  }

  private void throwIllegalException(String errorMessage) {
    throw new IllegalArgumentException(" Store '" + this.storeName + "'. Error: " + errorMessage);
  }

  public long getMemoryFootprintMB() {
    return this.memoryFootprintMB;
  }

  public boolean hasMemoryFootprint() {
    return memoryFootprintMB != 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    else if (o == null)
      return false;
    else if (!(o.getClass() == BdbStoreConfig.class))
      return false;

    BdbStoreConfig config = (BdbStoreConfig) o;
    return getName().equals(config.getName())
      && Objects.equal(getMemoryFootprintMB(), config.getMemoryFootprintMB());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(),
      getMemoryFootprintMB());
  }

  @Override
  public String toString() {
    return "StoreDefinition(name = " + getName() + ", memory-footprint(MB)" + getMemoryFootprintMB() + ")";
  }

  public String getName() {
    return this.storeName;
  }
}
