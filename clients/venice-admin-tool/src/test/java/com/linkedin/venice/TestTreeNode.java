package com.linkedin.venice;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA;
import static com.linkedin.venice.zk.VeniceZkPaths.EXECUTION_IDS;
import static com.linkedin.venice.zk.VeniceZkPaths.PARENT_OFFLINE_PUSHES;
import static com.linkedin.venice.zk.VeniceZkPaths.ROUTERS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_CONFIGS;
import static com.linkedin.venice.zk.VeniceZkPaths.STORE_GRAVEYARD;

import com.linkedin.venice.ZkCopier.TreeNode;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTreeNode {
  private static final String CLUSTER_1 = "cluster1";
  private static final String CLUSTER_2 = "cluster2";
  private static final String BASE_PATH = "/venice-parent";
  /** {@code root} is used in {@code testTreeNodeMethods()} to build the tree and then in {@code testTreeToList()} to convert the path tree to a list */
  private TreeNode root = new TreeNode(BASE_PATH);

  @Test
  public void testListToTree() {
    TreeNode testRoot = ZkCopier.pathsListToTree(getPaths(), BASE_PATH);
    Assert.assertEquals(testRoot.getChildren().size(), 3);
    Assert.assertTrue(testRoot.containsChild(STORE_CONFIGS));
    Assert.assertTrue(testRoot.containsChild(CLUSTER_1));
    Assert.assertTrue(testRoot.containsChild(CLUSTER_2));
    for (TreeNode child: testRoot.getChildren().values()) {
      switch (child.getName()) {
        case STORE_CONFIGS:
          Assert.assertEquals(child.getChildren().size(), 0);
          break;
        case CLUSTER_1:
          Assert.assertEquals(child.getChildren().size(), 6);
          testContainsChildAsserts(child);
          for (TreeNode grandchild: child.getChildren().values()) {
            if (grandchild.getName().equals(ADMIN_TOPIC_METADATA)) {
              Assert.assertEquals(grandchild.getChildren().size(), 2);
              for (TreeNode greatgrandchild: grandchild.getChildren().values()) {
                if (greatgrandchild.getName().equals("file2")) {
                  Assert.assertEquals(greatgrandchild.getChildren().size(), 1);
                }
              }
            }
          }
          break;
        case CLUSTER_2:
          Assert.assertEquals(child.getChildren().size(), 6);
          testContainsChildAsserts(child);
          break;
        default:
          Assert.fail("Unexpected child name: " + child.getName());
      }
    }
  }

  @Test
  public void testTreeNodeMethods() {
    Assert.assertEquals(root.getName(), BASE_PATH);
    Assert.assertEquals(root.getChildren().size(), 0);
    root.addChild(STORE_CONFIGS);
    root.addChild(CLUSTER_1);
    root.addChild(CLUSTER_2);
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
          Assert.assertEquals(child.getChildren().size(), 0);
          child.addChild(STORE_CONFIGS);
          child.addChild(ADMIN_TOPIC_METADATA);
          child.addChild(EXECUTION_IDS);
          child.addChild(PARENT_OFFLINE_PUSHES);
          child.addChild(ROUTERS);
          child.addChild(STORES);
          child.addChild(STORE_GRAVEYARD);
          Assert.assertEquals(child.getChildren().size(), 7);
          Assert.assertTrue(child.containsChild(STORE_CONFIGS));
          child.deleteChild(STORE_CONFIGS);
          Assert.assertEquals(child.getChildren().size(), 6);
          Assert.assertFalse(child.containsChild(STORE_CONFIGS));
          testContainsChildAsserts(child);
          for (TreeNode grandchild: child.getChildren().values()) {
            Assert.assertEquals(grandchild.getChildren().size(), 0);
            if (grandchild.getName().equals(ADMIN_TOPIC_METADATA)) {
              grandchild.addChild("file1");
              grandchild.addChild("file2");
              Assert.assertEquals(grandchild.getChildren().size(), 2);
              for (TreeNode greatgrandchild: grandchild.getChildren().values()) {
                Assert.assertEquals(greatgrandchild.getChildren().size(), 0);
                if (greatgrandchild.getName().equals("file2")) {
                  greatgrandchild.addChild("file3");
                  Assert.assertEquals(greatgrandchild.getChildren().size(), 1);
                }
              }
            }
          }
          break;
        case CLUSTER_2:
          Assert.assertEquals(child.getChildren().size(), 0);
          child.addChild(STORE_CONFIGS);
          child.addChild(ADMIN_TOPIC_METADATA);
          child.addChild(EXECUTION_IDS);
          child.addChild(PARENT_OFFLINE_PUSHES);
          child.addChild(ROUTERS);
          child.addChild(STORES);
          child.addChild(STORE_GRAVEYARD);
          Assert.assertEquals(child.getChildren().size(), 7);
          Assert.assertTrue(child.containsChild(STORE_CONFIGS));
          child.deleteChild(STORE_CONFIGS);
          Assert.assertEquals(child.getChildren().size(), 6);
          Assert.assertFalse(child.containsChild(STORE_CONFIGS));
          testContainsChildAsserts(child);
          break;
        default:
          Assert.fail("Unexpected child name: " + child.getName());
      }
    }
  }

  @Test
  public void testTreeToList() {
    List<String> list = ZkCopier.pathsTreeToList(root);
    Assert.assertEquals(list.size(), 19);
    Assert.assertTrue(list.contains("/venice-parent"));
    Assert.assertTrue(list.contains("/venice-parent/storeConfigs"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1"));
    Assert.assertFalse(list.contains("/venice-parent/cluster1/storeConfigs"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/adminTopicMetadata"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/adminTopicMetadata/file1"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/adminTopicMetadata/file2"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/adminTopicMetadata/file2/file3"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/executionids"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/ParentOfflinePushes"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/routers"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/StoreGraveyard"));
    Assert.assertTrue(list.contains("/venice-parent/cluster1/Stores"));
    Assert.assertTrue(list.contains("/venice-parent/cluster2"));
    Assert.assertFalse(list.contains("/venice-parent/cluster2/storeConfigs"));
    Assert.assertTrue(list.contains("/venice-parent/cluster2/adminTopicMetadata"));
    Assert.assertTrue(list.contains("/venice-parent/cluster2/executionids"));
    Assert.assertTrue(list.contains("/venice-parent/cluster2/ParentOfflinePushes"));
    Assert.assertTrue(list.contains("/venice-parent/cluster2/routers"));
    Assert.assertTrue(list.contains("/venice-parent/cluster2/StoreGraveyard"));
    Assert.assertTrue(list.contains("/venice-parent/cluster2/Stores"));
  }

  public void testContainsChildAsserts(TreeNode child) {
    Assert.assertTrue(child.containsChild(ADMIN_TOPIC_METADATA));
    Assert.assertTrue(child.containsChild(EXECUTION_IDS));
    Assert.assertTrue(child.containsChild(PARENT_OFFLINE_PUSHES));
    Assert.assertTrue(child.containsChild(ROUTERS));
    Assert.assertTrue(child.containsChild(STORES));
    Assert.assertTrue(child.containsChild(STORE_GRAVEYARD));
  }

  public List<String> getPaths() {
    List<String> paths = new ArrayList<>();
    paths.add("/venice-parent/storeConfigs");
    paths.add("/venice-parent/cluster1");
    paths.add("/venice-parent/cluster1/adminTopicMetadata");
    paths.add("/venice-parent/cluster1/adminTopicMetadata/file1");
    paths.add("/venice-parent/cluster1/adminTopicMetadata/file2/file3"); // tests both /file2 and /file2/file3
    paths.add("/venice-parent/cluster1/executionids");
    paths.add("/venice-parent/cluster1/ParentOfflinePushes");
    paths.add("/venice-parent/cluster1/routers");
    paths.add("/venice-parent/cluster1/StoreGraveyard");
    paths.add("/venice-parent/cluster1/Stores");
    paths.add("/venice-parent/cluster2");
    paths.add("/venice-parent/cluster2/adminTopicMetadata");
    paths.add("/venice-parent/cluster2/executionids");
    paths.add("/venice-parent/cluster2/ParentOfflinePushes");
    paths.add("/venice-parent/cluster2/routers");
    paths.add("/venice-parent/cluster2/StoreGraveyard");
    paths.add("/venice-parent/cluster2/Stores");
    return paths;
  }
}
