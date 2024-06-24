package com.linkedin.venice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCloneVeniceZKPaths {
  private final Set<String> CLUSTERS = new HashSet<>(Arrays.asList("cluster1", "cluster2"));
  private final String BASE_PATH = "/venice-parent";

  @Test
  public void mainTest() {
    testBuildRequiredPathsTree();
    testGetVenicePaths();
    testExtractVenicePaths();
    // testCloneVenicePaths();
  }

  public void testCloneVenicePaths() {
    // TODO write tests for cloneVenicePaths()
  }

  public void testExtractVenicePaths() {
    // change this variable to path of where Venice project is stored
    String directory = "/Users/tonchen/Desktop/";
    String inputPath = directory + "venice/clients/venice-admin-tool/src/test/java/com/linkedin/venice/zk_paths.txt";
    String outputPath =
        directory + "venice/clients/venice-admin-tool/src/test/java/com/linkedin/venice/venice_paths.txt";
    CloneVeniceZKPaths.extractVenicePaths(inputPath, outputPath, CLUSTERS, BASE_PATH);
    // look at venice_paths.txt and verify that all (19) Venice-specific paths are extracted correctly
  }

  public void testGetVenicePaths() {
    ArrayList<String> zkPaths = getPaths();
    Set<String> requiredPaths = CloneVeniceZKPaths.getRequiredPaths();
    ArrayList<String> venicePaths = CloneVeniceZKPaths.getVenicePaths(zkPaths, CLUSTERS, BASE_PATH, requiredPaths);
    testVenicePathsAsserts(venicePaths);
  }

  public void testVenicePathsAsserts(ArrayList<String> venicePaths) {
    Assert.assertEquals(venicePaths.size(), 21);
    Assert.assertFalse(venicePaths.contains("/venice-parent"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster1/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadata"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadata/file1"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadata/file2"));
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
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids/file2"));
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

  public void testBuildRequiredPathsTree() {
    ZNode root = CloneVeniceZKPaths.buildRequiredPathsTree(CLUSTERS, BASE_PATH, CloneVeniceZKPaths.getRequiredPaths());
    Assert.assertEquals(root.getChildren().size(), 3);
    Assert.assertTrue(root.contains("storeConfigs"));
    Assert.assertTrue(root.contains("cluster1"));
    Assert.assertTrue(root.contains("cluster2"));
    for (ZNode child: root.getChildren()) {
      switch (child.getVal()) {
        case "storeConfigs":
          Assert.assertEquals(child.getPath(), "/venice-parent/storeConfigs");
          Assert.assertEquals(child.getChildren().size(), 0);
          break;
        case "cluster1":
          Assert.assertEquals(child.getPath(), "/venice-parent/cluster1");
          Assert.assertEquals(child.getChildren().size(), 6);
          Assert.assertTrue(child.contains("adminTopicMetadata"));
          Assert.assertTrue(child.contains("executionids"));
          Assert.assertTrue(child.contains("ParentOfflinePushes"));
          Assert.assertTrue(child.contains("routers"));
          Assert.assertTrue(child.contains("StoreGraveyard"));
          Assert.assertTrue(child.contains("Stores"));
          for (ZNode grandchild: child.getChildren()) {
            Assert.assertEquals(grandchild.getPath(), "/venice-parent/cluster1/" + grandchild.getVal());
            Assert.assertEquals(grandchild.getChildren().size(), 0);
          }
          break;
        case "cluster2":
          Assert.assertEquals(child.getPath(), "/venice-parent/cluster2");
          Assert.assertEquals(child.getChildren().size(), 6);
          Assert.assertTrue(child.contains("adminTopicMetadata"));
          Assert.assertTrue(child.contains("executionids"));
          Assert.assertTrue(child.contains("ParentOfflinePushes"));
          Assert.assertTrue(child.contains("routers"));
          Assert.assertTrue(child.contains("StoreGraveyard"));
          Assert.assertTrue(child.contains("Stores"));
          for (ZNode grandchild: child.getChildren()) {
            Assert.assertEquals(grandchild.getPath(), "/venice-parent/cluster2/" + grandchild.getVal());
            Assert.assertEquals(grandchild.getChildren().size(), 0);
          }
          break;
      }
    }
  }

  public ArrayList<String> getPaths() {
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
