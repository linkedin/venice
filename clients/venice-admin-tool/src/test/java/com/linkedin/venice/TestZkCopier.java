package com.linkedin.venice;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.zk.VeniceZkPaths.EXECUTION_IDS;
import static com.linkedin.venice.zk.VeniceZkPaths.PARENT_OFFLINE_PUSHES;
import static com.linkedin.venice.zk.VeniceZkPaths.ROUTERS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_GRAVEYARD;

import com.linkedin.venice.ZkCopier.TreeNode;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkCopier {
  private final String CLUSTER_1 = "cluster1";
  private final String CLUSTER_2 = "cluster2";
  private final Set<String> CLUSTERS = new HashSet<>(Arrays.asList(CLUSTER_1, CLUSTER_2));
  private final String BASE_PATH = "/venice-parent";

  /**
   * Test AdminTool.extractVeniceZKPaths() and ZkCopier.extractVenicePaths()
   */
  @Test
  public void testExtractVenicePaths() throws Exception {
    URL zkResource = TestZkCopier.class.getClassLoader().getResource("zk_paths.txt");
    File zkResourceFile = Paths.get(zkResource.toURI()).toFile();
    String inputPath = zkResourceFile.getAbsolutePath();
    URL veniceResource = TestZkCopier.class.getClassLoader().getResource("venice_paths.txt");
    File veniceResourceFile = Paths.get(veniceResource.toURI()).toFile();
    File outputDir = Utils.getTempDataDirectory();
    File outputFile = new File(outputDir, "test_outputs.txt");
    String outputPath = outputFile.getAbsolutePath();
    String[] args = { "--extract-venice-zk-paths", "--infile", inputPath, "--outfile", outputPath, "--cluster-list",
        "cluster1, cluster2", "--base-path", BASE_PATH };
    try {
      AdminTool.main(args);
      boolean isFileEqual = FileUtils.contentEquals(veniceResourceFile, outputFile);
      Assert.assertTrue(isFileEqual);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetVenicePathsFromList() {
    List<String> zkPaths = getPaths();
    List<String> venicePaths = ZkCopier.getVenicePathsFromList(zkPaths, CLUSTERS, BASE_PATH);
    Assert.assertEquals(venicePaths.size(), 22);
    Assert.assertFalse(venicePaths.contains("/venice"));
    Assert.assertFalse(venicePaths.contains("/venice/storeConfigs"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/storeConfigs"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/adminTopicMetadataV2"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/adminTopicMetadataV2/file1"));
    Assert.assertFalse(venicePaths.contains("/venice/cluster1/adminTopicMetadataV2/file2/file3"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster1/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadataV2/file1"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadataV2/file2"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadataV2/file2/file3"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/cluster2/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids/file1"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids/file2"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids/file2/file3"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/helix-cluster"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/helix-cluster/storeConfigs"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/helix-cluster/adminTopicMetadataV2"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/helix-cluster/adminTopicMetadataV2/file1"));
    Assert.assertFalse(venicePaths.contains("/venice-parent/helix-cluster/adminTopicMetadataV2/file2/file3"));
    testVenicePathsContainsAsserts(venicePaths);
  }

  @Test
  public void testGetVenicePathsFromTree() {
    TreeNode root = ZkCopier.buildRequiredPathsTree(CLUSTERS, BASE_PATH);
    List<String> venicePaths = ZkCopier.getVenicePathsFromTree(root, CLUSTERS, BASE_PATH);
    testVenicePathsContainsAsserts(venicePaths);
  }

  @Test
  public void testBuildRequiredPathsTree() {
    TreeNode root = ZkCopier.buildRequiredPathsTree(CLUSTERS, BASE_PATH);
    Assert.assertEquals(root.getChildren().size(), 3);
    Assert.assertTrue(root.containsChild(STORE_CONFIGS));
    Assert.assertTrue(root.containsChild(CLUSTER_1));
    Assert.assertTrue(root.containsChild(CLUSTER_2));
    for (TreeNode child: root.getChildren().values()) {
      switch (child.getName()) {
        case STORE_CONFIGS:
          Assert.assertEquals(child.getChildren().size(), 0);
          break;
        case CLUSTER_1:
        case CLUSTER_2:
          Assert.assertEquals(child.getChildren().size(), 6);
          testContainsChildAsserts(child);
          for (TreeNode grandchild: child.getChildren().values()) {
            Assert.assertEquals(grandchild.getChildren().size(), 0);
          }
          break;
      }
    }
  }

  private void testVenicePathsContainsAsserts(List<String> venicePaths) {
    Assert.assertTrue(venicePaths.contains("/venice-parent"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/storeConfigs"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/adminTopicMetadataV2"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/executionids"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/ParentOfflinePushes"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/routers"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/StoreGraveyard"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster1/Stores"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/adminTopicMetadataV2"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/executionids"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/ParentOfflinePushes"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/routers"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/StoreGraveyard"));
    Assert.assertTrue(venicePaths.contains("/venice-parent/cluster2/Stores"));
  }

  private void testContainsChildAsserts(TreeNode child) {
    Assert.assertTrue(child.containsChild(ADMIN_TOPIC_METADATA));
    Assert.assertTrue(child.containsChild(EXECUTION_IDS));
    Assert.assertTrue(child.containsChild(PARENT_OFFLINE_PUSHES));
    Assert.assertTrue(child.containsChild(ROUTERS));
    Assert.assertTrue(child.containsChild(STORES));
    Assert.assertTrue(child.containsChild(STORE_GRAVEYARD));
  }

  private List<String> getPaths() {
    List<String> zkPaths = new ArrayList<>();
    zkPaths.add("/venice");
    zkPaths.add("/venice/storeConfigs");
    zkPaths.add("/venice/cluster1");
    zkPaths.add("/venice/cluster1/storeConfigs");
    zkPaths.add("/venice/cluster1/adminTopicMetadataV2");
    zkPaths.add("/venice/cluster1/adminTopicMetadataV2/file1");
    zkPaths.add("/venice/cluster1/adminTopicMetadataV2/file2/file3");
    zkPaths.add("/venice-parent");
    zkPaths.add("/venice-parent/storeConfigs");
    zkPaths.add("/venice-parent/cluster1");
    zkPaths.add("/venice-parent/cluster1/storeConfigs");
    zkPaths.add("/venice-parent/cluster1/adminTopicMetadataV2");
    zkPaths.add("/venice-parent/cluster1/adminTopicMetadataV2/file1");
    zkPaths.add("/venice-parent/cluster1/adminTopicMetadataV2/file2/file3");
    zkPaths.add("/venice-parent/cluster1/executionids");
    zkPaths.add("/venice-parent/cluster1/ParentOfflinePushes");
    zkPaths.add("/venice-parent/cluster1/routers");
    zkPaths.add("/venice-parent/cluster1/StoreGraveyard");
    zkPaths.add("/venice-parent/cluster1/Stores");
    zkPaths.add("/venice-parent/cluster2");
    zkPaths.add("/venice-parent/cluster2/storeConfigs");
    zkPaths.add("/venice-parent/cluster2/adminTopicMetadataV2");
    zkPaths.add("/venice-parent/cluster2/executionids");
    zkPaths.add("/venice-parent/cluster2/executionids/file1");
    zkPaths.add("/venice-parent/cluster2/executionids/file2/file3");
    zkPaths.add("/venice-parent/cluster2/ParentOfflinePushes");
    zkPaths.add("/venice-parent/cluster2/routers");
    zkPaths.add("/venice-parent/cluster2/StoreGraveyard");
    zkPaths.add("/venice-parent/cluster2/Stores");
    zkPaths.add("/venice-parent/helix-cluster");
    zkPaths.add("/venice-parent/helix-cluster/storeConfigs");
    zkPaths.add("/venice-parent/helix-cluster/adminTopicMetadataV2");
    zkPaths.add("/venice-parent/helix-cluster/adminTopicMetadataV2/file1");
    zkPaths.add("/venice-parent/helix-cluster/adminTopicMetadataV2/file2/file3");
    return zkPaths;
  }
}
