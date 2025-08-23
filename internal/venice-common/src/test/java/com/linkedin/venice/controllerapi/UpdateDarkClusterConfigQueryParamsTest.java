package com.linkedin.venice.controllerapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class UpdateDarkClusterConfigQueryParamsTest {
  @Test
  public void testSetAndGetTargetStores() {
    Map<String, String> params = new HashMap<>();
    UpdateDarkClusterConfigQueryParams emptyQueryParams = new UpdateDarkClusterConfigQueryParams(params);
    Assert.assertEquals(emptyQueryParams.getTargetStores(), Optional.empty());

    UpdateDarkClusterConfigQueryParams queryParams = new UpdateDarkClusterConfigQueryParams(params);
    queryParams.setTargetStores(Arrays.asList("a", "b", "c"));
    Assert.assertEquals(queryParams.getTargetStores(), Optional.of(Arrays.asList("a", "b", "c")));

    UpdateDarkClusterConfigQueryParams emptyStoresQueryParams = new UpdateDarkClusterConfigQueryParams(params);
    emptyStoresQueryParams.setTargetStores(new ArrayList<>());
    Assert.assertEquals(emptyStoresQueryParams.getTargetStores(), Optional.of(new ArrayList<>()));
  }
}
