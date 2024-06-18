package com.linkedin.venice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCloneVeniceZKPaths {
  @Test
  public void testGetVenicePaths() {
    ArrayList<String> zkPaths = getPaths();
    Set<String> clusters = new HashSet<>(Arrays.asList("cluster1", "cluster2"));
    String basePath = "/venice-parent";
    Set<String> requiredPaths = new HashSet<>(
        Arrays.asList(
            "adminTopicMetadata",
            "executionids",
            "ParentOfflinePushes",
            "routers",
            "StoreGraveyard",
            "Stores"));

    ArrayList<String> venicePaths = CloneVeniceZKPaths.getVenicePaths(zkPaths, clusters, basePath, requiredPaths);

    Assert.assertEquals(venicePaths.size(), 19);
    Assert.assertFalse(venicePaths.contains("/venice-parent"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster1/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadata"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadata/file1"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadata/file2/file3"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/executionids"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/ParentOfflinePushes"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/routers"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/StoreGraveyard"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/Stores"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster2/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/adminTopicMetadata"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids/file1"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids/file2/file3"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/ParentOfflinePushes"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/routers"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/StoreGraveyard"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/Stores"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster3"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster3/storeConfigs"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster3/adminTopicMetadata"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster3/adminTopicMetadata/file1"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster3/adminTopicMetadata/file2/file3"));
    Assert.assertFalse(venicePaths.contains("/venice"));
    Assert.assertFalse(venicePaths.contains("/venice/storeConfigs"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/storeConfigs"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/adminTopicMetadata"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/adminTopicMetadata/file1"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/adminTopicMetadata/file2/file3"));
  }

  private static ArrayList<String> getPaths() {
    ArrayList<String> zkPaths = new ArrayList<>();
    zkPaths.add("/venice-parent");
    zkPaths.add("/venice-parent/storeConfigs");
    zkPaths.add("/venice-parent/cluster1");
    zkPaths.add("/venice-parent/cluster1/storeConfigs");
    zkPaths.add("/venice-parent/cluster1/adminTopicMetadata");
    zkPaths.add("/venice-parent/cluster1/adminTopicMetadata/file1");
    zkPaths.add("/venice-parent/cluster1/adminTopicMetadata/file2/file3");
    zkPaths.add("/venice-parent/cluster1/executionids");
    zkPaths.add("/venice-parent/cluster1/ParentOfflinePushes");
    zkPaths.add("/venice-parent/cluster1/routers");
    zkPaths.add("/venice-parent/cluster1/StoreGraveyard");
    zkPaths.add("/venice-parent/cluster1/Stores");
    zkPaths.add("/venice-parent/cluster2");
    zkPaths.add("/venice-parent/cluster2/storeConfigs");
    zkPaths.add("/venice-parent/cluster2/adminTopicMetadata");
    zkPaths.add("/venice-parent/cluster2/executionids");
    zkPaths.add("/venice-parent/cluster2/executionids/file1");
    zkPaths.add("/venice-parent/cluster2/executionids/file2/file3");
    zkPaths.add("/venice-parent/cluster2/ParentOfflinePushes");
    zkPaths.add("/venice-parent/cluster2/routers");
    zkPaths.add("/venice-parent/cluster2/StoreGraveyard");
    zkPaths.add("/venice-parent/cluster2/Stores");
    zkPaths.add("/venice-parent/cluster3");
    zkPaths.add("/venice-parent/cluster3/storeConfigs");
    zkPaths.add("/venice-parent/cluster3/adminTopicMetadata");
    zkPaths.add("/venice-parent/cluster3/adminTopicMetadata/file1");
    zkPaths.add("/venice-parent/cluster3/adminTopicMetadata/file2/file3");
    zkPaths.add("/venice");
    zkPaths.add("/venice/storeConfigs");
    zkPaths.add("/venice/cluster1");
    zkPaths.add("/venice/cluster1/storeConfigs");
    zkPaths.add("/venice/cluster1/adminTopicMetadata");
    zkPaths.add("/venice/cluster1/adminTopicMetadata/file1");
    zkPaths.add("/venice/cluster1/adminTopicMetadata/file2/file3");
    return zkPaths;
  }
}
