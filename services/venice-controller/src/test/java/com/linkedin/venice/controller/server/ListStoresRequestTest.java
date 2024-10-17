package com.linkedin.venice.controller.server;

import static org.testng.Assert.*;

import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ListStoresRequestTest {
  private ListStoresRequest _mockListStoresRequest;

  private final String TEST_CLUSTER = "testCluster";
  private final String TEST_STORE_NAME = "testStoreName";
  private final String TEST_INCLUDE_SYSTEM_STORES = "testIncludeSystemStores";
  private final String TEST_STORE_CONFIG_NAME_FILTER = "testStoreConfigNameFilter";
  private final String TEST_STORE_CONFIG_VALUE_FILTER = "testStoreConfigValueFilter";

  @BeforeMethod
  public void setUp() {
    // Create a mock ListStoresRequestTest with mock data for each of it's private fields:
    _mockListStoresRequest = new ListStoresRequest(
        "cluster",
        "storeName",
        "includeSystemStores",
        Optional.of("storeConfigNameFilter"),
        Optional.of("storeConfigValueFilter"));
  }

  @Test
  public void testGetCluster() {
    assertEquals(_mockListStoresRequest.getCluster(), "cluster");
  }

  @Test
  public void testGetStoreName() {
    assertEquals(_mockListStoresRequest.getStoreName(), "storeName");
  }

  @Test
  public void testSetCluster() {
    _mockListStoresRequest.setCluster(TEST_CLUSTER);
    assertEquals(_mockListStoresRequest.getCluster(), TEST_CLUSTER);
  }

  @Test
  public void testSetStoreName() {
    _mockListStoresRequest.setStoreName(TEST_STORE_NAME);
    assertEquals(_mockListStoresRequest.getStoreName(), TEST_STORE_NAME);
  }

  @Test
  public void testGetIncludeSystemStores() {
    assertEquals(_mockListStoresRequest.getIncludeSystemStores(), "includeSystemStores");
  }

  @Test
  public void testSetIncludeSystemStores() {
    _mockListStoresRequest.setIncludeSystemStores(TEST_INCLUDE_SYSTEM_STORES);
    assertEquals(_mockListStoresRequest.getIncludeSystemStores(), TEST_INCLUDE_SYSTEM_STORES);
  }

  @Test
  public void testGetStoreConfigNameFilter() {
    assertEquals(_mockListStoresRequest.getStoreConfigNameFilter(), Optional.of("storeConfigNameFilter"));
  }

  @Test
  public void testSetStoreConfigNameFilter() {
    _mockListStoresRequest.setStoreConfigNameFilter(Optional.of(TEST_STORE_CONFIG_NAME_FILTER));
    assertEquals(_mockListStoresRequest.getStoreConfigNameFilter(), Optional.of(TEST_STORE_CONFIG_NAME_FILTER));
  }

  @Test
  public void testGetStoreConfigValueFilter() {
    assertEquals(_mockListStoresRequest.getStoreConfigValueFilter(), Optional.of("storeConfigValueFilter"));
  }

  @Test
  public void testSetStoreConfigValueFilter() {
    _mockListStoresRequest.setStoreConfigValueFilter(Optional.of(TEST_STORE_CONFIG_VALUE_FILTER));
    assertEquals(_mockListStoresRequest.getStoreConfigValueFilter(), Optional.of(TEST_STORE_CONFIG_VALUE_FILTER));
  }
}
