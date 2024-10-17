package com.linkedin.venice.controller.server;

import static org.testng.Assert.*;

import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ListAllStoresStatusesRequestTest {
  private ListAllStoresStatusesRequest mockListAllStoresStatusesRequest;

  private final String TEST_CLUSTER = "testCluster";
  private final String TEST_STORE_NAME = "testStoreName";
  private final String TEST_INCLUDE_SYSTEM_STORES = "testIncludeSystemStores";
  private final String TEST_STORE_CONFIG_NAME_FILTER = "testStoreConfigNameFilter";
  private final String TEST_STORE_CONFIG_VALUE_FILTER = "testStoreConfigValueFilter";

  @BeforeMethod
  public void setUp() {
    // Create a mock ListStoresRequestTest with mock data for each of it's private fields:
    mockListAllStoresStatusesRequest = new ListAllStoresStatusesRequest(
        "cluster",
        "storeName",
        "includeSystemStores",
        Optional.of("storeConfigNameFilter"),
        Optional.of("storeConfigValueFilter"));
  }

  @Test
  public void testGetCluster() {
    assertEquals(mockListAllStoresStatusesRequest.getCluster(), "cluster");
  }

  @Test
  public void testGetStoreName() {
    assertEquals(mockListAllStoresStatusesRequest.getStoreName(), "storeName");
  }

  @Test
  public void testSetCluster() {
    mockListAllStoresStatusesRequest.setCluster(TEST_CLUSTER);
    assertEquals(mockListAllStoresStatusesRequest.getCluster(), TEST_CLUSTER);
  }

  @Test
  public void testSetStoreName() {
    mockListAllStoresStatusesRequest.setStoreName(TEST_STORE_NAME);
    assertEquals(mockListAllStoresStatusesRequest.getStoreName(), TEST_STORE_NAME);
  }

  @Test
  public void testGetIncludeSystemStores() {
    assertEquals(mockListAllStoresStatusesRequest.getIncludeSystemStores(), "includeSystemStores");
  }

  @Test
  public void testSetIncludeSystemStores() {
    mockListAllStoresStatusesRequest.setIncludeSystemStores(TEST_INCLUDE_SYSTEM_STORES);
    assertEquals(mockListAllStoresStatusesRequest.getIncludeSystemStores(), TEST_INCLUDE_SYSTEM_STORES);
  }

  @Test
  public void testGetStoreConfigNameFilter() {
    assertEquals(mockListAllStoresStatusesRequest.getStoreConfigNameFilter(), Optional.of("storeConfigNameFilter"));
  }

  @Test
  public void testSetStoreConfigNameFilter() {
    mockListAllStoresStatusesRequest.setStoreConfigNameFilter(Optional.of(TEST_STORE_CONFIG_NAME_FILTER));
    assertEquals(
        mockListAllStoresStatusesRequest.getStoreConfigNameFilter(),
        Optional.of(TEST_STORE_CONFIG_NAME_FILTER));
  }

  @Test
  public void testGetStoreConfigValueFilter() {
    assertEquals(mockListAllStoresStatusesRequest.getStoreConfigValueFilter(), Optional.of("storeConfigValueFilter"));
  }

  @Test
  public void testSetStoreConfigValueFilter() {
    mockListAllStoresStatusesRequest.setStoreConfigValueFilter(Optional.of(TEST_STORE_CONFIG_VALUE_FILTER));
    assertEquals(
        mockListAllStoresStatusesRequest.getStoreConfigValueFilter(),
        Optional.of(TEST_STORE_CONFIG_VALUE_FILTER));
  }
}
