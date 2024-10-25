package com.linkedin.venice.controllerapi.request;

import com.linkedin.venice.meta.systemstore.schemas.StoreProperties;
import org.apache.avro.Schema;


public class ListStoresRequest extends ControllerRequest {
  private final boolean excludeSystemStores;
  private final String storeConfigNameFilter;
  private final String storeConfigValueFilter;
  private final boolean isDataReplicationPolicyConfigFilter;
  private final Schema.Field configFilterField;

  public ListStoresRequest(
      String clusterName,
      boolean excludeSystemStores,
      String storeConfigNameFilter,
      String storeConfigValueFilter) {
    super(clusterName);
    this.excludeSystemStores = excludeSystemStores;
    this.storeConfigNameFilter = storeConfigNameFilter;
    this.storeConfigValueFilter = storeConfigValueFilter;

    validateFilters(storeConfigNameFilter, storeConfigValueFilter);
    this.configFilterField = initializeConfigFilterField(storeConfigNameFilter);
    this.isDataReplicationPolicyConfigFilter = checkDataReplicationPolicyConfigFilter(storeConfigNameFilter);
  }

  private void validateFilters(String storeConfigNameFilter, String storeConfigValueFilter) {
    boolean isNameFilterNull = storeConfigNameFilter == null;
    boolean isValueFilterNull = storeConfigValueFilter == null;

    if (isNameFilterNull != isValueFilterNull) {
      throw new IllegalArgumentException(
          "Missing parameter: " + (isValueFilterNull ? "store_config_name_filter" : "store_config_value_filter"));
    }
  }

  private Schema.Field initializeConfigFilterField(String storeConfigNameFilter) {
    if (storeConfigNameFilter != null) {
      Schema.Field field = StoreProperties.getClassSchema().getField(storeConfigNameFilter);
      if (field == null && !storeConfigNameFilter.equalsIgnoreCase("dataReplicationPolicy")) {
        throw new IllegalArgumentException("The config name filter " + storeConfigNameFilter + " is not valid.");
      }
      return field;
    }
    return null; // Return null if storeConfigNameFilter is not set
  }

  private boolean checkDataReplicationPolicyConfigFilter(String storeConfigNameFilter) {
    return storeConfigNameFilter != null && storeConfigNameFilter.equalsIgnoreCase("dataReplicationPolicy");
  }

  public boolean isExcludeSystemStores() {
    return excludeSystemStores;
  }

  public boolean includeSystemStores() {
    return !excludeSystemStores;
  }

  public String getStoreConfigNameFilter() {
    return storeConfigNameFilter;
  }

  public String getStoreConfigValueFilter() {
    return storeConfigValueFilter;
  }

  public boolean isDataReplicationPolicyConfigFilter() {
    return isDataReplicationPolicyConfigFilter;
  }

  public Schema.Field getConfigFilterField() {
    return configFilterField;
  }
}
