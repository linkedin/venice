package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;


public class BatchGetRequestContextTest {
  @Test
  public void testCreateRetryRequestContext() {
    BatchGetRequestContext originalRequestContext = new BatchGetRequestContext(10, true);
    Map<Integer, Set<String>> originalRoutesForPartitionMapping = new HashMap<>();
    originalRequestContext.currentVersion = 1;
    originalRequestContext.routeRequestMap.put("route1", new CompletableFuture<>());
    originalRequestContext.helixGroupId = 1;
    originalRoutesForPartitionMapping.put(0, new HashSet<>(Arrays.asList("route1")));
    originalRoutesForPartitionMapping.put(1, new HashSet<>(Arrays.asList("route2")));
    originalRequestContext.setRoutesForPartitionMapping(originalRoutesForPartitionMapping);

    BatchGetRequestContext retryRequestContext = originalRequestContext.createRetryRequestContext(3);
    assertTrue(retryRequestContext.retryRequest);
    assertEquals(retryRequestContext.helixGroupId, 1);
    assertEquals(retryRequestContext.currentVersion, 1);
    assertEquals(retryRequestContext.numKeysInRequest, 3);
    assertEquals(retryRequestContext.getRoutesForPartitionMapping().size(), 2);
    assertEquals(retryRequestContext.getRouteRequestMap().size(), 1);
  }
}
