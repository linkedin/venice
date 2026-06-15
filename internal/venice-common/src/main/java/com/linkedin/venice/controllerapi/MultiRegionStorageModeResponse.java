package com.linkedin.venice.controllerapi;

import java.util.HashMap;
import java.util.Map;


/**
 * Response for {@link ControllerRoute#GET_PER_REGION_STORAGE_MODE}: each region's store-level
 * {@link com.linkedin.venice.meta.StorageMode}, keyed by region name with the {@code StorageMode}'s enum name
 * as the value. Returned by the parent controller (one entry per child region) or by a child controller (a
 * single entry for its own region). Used by VPJ to decide, per region, whether to dual-write to external
 * storage.
 */
public class MultiRegionStorageModeResponse extends ControllerResponse {
  private Map<String, String> regionToStorageMode = new HashMap<>();

  public Map<String, String> getRegionToStorageMode() {
    return regionToStorageMode;
  }

  public void setRegionToStorageMode(Map<String, String> regionToStorageMode) {
    this.regionToStorageMode = regionToStorageMode;
  }

  @Override
  public String toString() {
    return "MultiRegionStorageModeResponse{regionToStorageMode=" + regionToStorageMode + '}';
  }
}
