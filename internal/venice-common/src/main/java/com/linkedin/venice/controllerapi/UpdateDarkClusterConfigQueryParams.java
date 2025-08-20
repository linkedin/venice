package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.ConfigKeys.DARK_CLUSTER_TARGET_STORES;

import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class UpdateDarkClusterConfigQueryParams extends QueryParams {
  public UpdateDarkClusterConfigQueryParams(Map<String, String> initialParams) {
    super(initialParams);
  }

  public UpdateDarkClusterConfigQueryParams() {
    super();
  }

  public Optional<Set<String>> getStoresSet() {
    String stores = getString(DARK_CLUSTER_TARGET_STORES).orElse(null);
    if (stores != null) {
      return Optional.of(com.linkedin.venice.utils.Utils.parseCommaSeparatedStringToSet(stores));
    }
    return Optional.empty();
  }

  public UpdateDarkClusterConfigQueryParams setStoresSet(Set<String> storesSet) {
    return (UpdateDarkClusterConfigQueryParams) putStringSet(DARK_CLUSTER_TARGET_STORES, storesSet);
  }

  // Add more getters and setters as needed for dark cluster specific configs
}
