package com.linkedin.venice.controller.server;

import java.util.Optional;


public class ListAllStoresStatusesRequest {
  private String cluster;
  private String storeName;
  private String includeSystemStores;
  private Optional<String> storeConfigNameFilter;
  private Optional<String> storeConfigValueFilter;

  public ListAllStoresStatusesRequest(
      String cluster,
      String storeName,
      String includeSystemStores,
      Optional<String> storeConfigNameFilter,
      Optional<String> storeConfigValueFilter) {
    this.cluster = cluster;
    this.storeName = storeName;
    this.includeSystemStores = includeSystemStores;
    this.storeConfigNameFilter = storeConfigNameFilter;
    this.storeConfigValueFilter = storeConfigValueFilter;
  }

  public ListAllStoresStatusesRequest() {
  }

  public String getCluster() {
    return cluster;
  }

  public String getStoreName() {
    return storeName;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public void setStoreName(String storeName) {
    this.storeName = storeName;
  }

  public String getIncludeSystemStores() {
    return includeSystemStores;
  }

  public void setIncludeSystemStores(String includeSystemStores) {
    this.includeSystemStores = includeSystemStores;
  }

  public Optional<String> getStoreConfigNameFilter() {
    return storeConfigNameFilter;
  }

  public void setStoreConfigNameFilter(Optional<String> storeConfigNameFilter) {
    this.storeConfigNameFilter = storeConfigNameFilter;
  }

  public Optional<String> getStoreConfigValueFilter() {
    return storeConfigValueFilter;
  }

  public void setStoreConfigValueFilter(Optional<String> storeConfigValueFilter) {
    this.storeConfigValueFilter = storeConfigValueFilter;
  }
}
