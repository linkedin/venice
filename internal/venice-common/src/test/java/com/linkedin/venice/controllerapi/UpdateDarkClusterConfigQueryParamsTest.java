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
  public void testSetAndGetStoresToReplicate() {
    Map<String, String> params = new HashMap<>();
    UpdateDarkClusterConfigQueryParams emptyQueryParams = new UpdateDarkClusterConfigQueryParams(params);
    Assert.assertEquals(emptyQueryParams.getStoresToReplicate(), Optional.empty());

    UpdateDarkClusterConfigQueryParams queryParams = new UpdateDarkClusterConfigQueryParams(params);
    queryParams.setStoresToReplicate(Arrays.asList("a", "b", "c"));
    Assert.assertEquals(queryParams.getStoresToReplicate(), Optional.of(Arrays.asList("a", "b", "c")));

    UpdateDarkClusterConfigQueryParams emptyStoresQueryParams = new UpdateDarkClusterConfigQueryParams(params);
    emptyStoresQueryParams.setStoresToReplicate(new ArrayList<>());
    Assert.assertEquals(emptyStoresQueryParams.getStoresToReplicate(), Optional.of(new ArrayList<>()));
  }
}
