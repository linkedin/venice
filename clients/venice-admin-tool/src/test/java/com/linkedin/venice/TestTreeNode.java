package com.linkedin.venice;

import static com.linkedin.venice.zk.VeniceZkPaths.ADMIN_TOPIC_METADATA_V2;
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

  @Test
  public void testPathsListToTree() {
    TreeNode root = ZkCopier.pathsListToTree(getPaths(), BASE_PATH);
    testTreeAsserts(root);
  }

  @Test
  public void testTreeNodeMethods() {
    TreeNode root = buildTestTree();
    testTreeAsserts(root);

    for (TreeNode child: root.getChildren().values()) {
      switch (child.getName()) {
        case STORE_CONFIGS:
          Assert.assertEquals(child.getChildren().size(), 0);
          child.addChild("newChild");
          Assert.assertEquals(child.getChildren().size(), 1);
          Assert.assertTrue(child.containsChild("newChild"));
          child.deleteChild("newChild");
          Assert.assertEquals(child.getChildren().size(), 0);
          break;
        case CLUSTER_1:
        case CLUSTER_2:
          Assert.assertEquals(child.getChildren().size(), 6);
          Assert.assertFalse(child.containsChild(STORE_CONFIGS));
          child.addChild("newChild");
          Assert.assertEquals(child.getChildren().size(), 7);
          Assert.assertTrue(child.containsChild("newChild"));
          child.deleteChild("newChild");
          Assert.assertEquals(child.getChildren().size(), 6);
          break;
        default:
          Assert.fail("Unexpected child name: " + child.getName());
      }
    }
  }

  @Test
  public void testPathsTreeToList() {
    TreeNode root = buildTestTree();
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

  private TreeNode buildTestTree() {
    TreeNode root = new TreeNode(BASE_PATH);
    root.addChild(STORE_CONFIGS);
    TreeNode cluster1 = root.addChild(CLUSTER_1);
    TreeNode adminTopicMetadata = cluster1.addChild(ADMIN_TOPIC_METADATA_V2);
    adminTopicMetadata.addChild("file1");
    TreeNode file2 = adminTopicMetadata.addChild("file2");
    file2.addChild("file3");
    cluster1.addChild(EXECUTION_IDS);
    cluster1.addChild(PARENT_OFFLINE_PUSHES);
    cluster1.addChild(ROUTERS);
    cluster1.addChild(STORES);
    cluster1.addChild(STORE_GRAVEYARD);
    TreeNode cluster2 = root.addChild(CLUSTER_2);
    cluster2.addChild(ADMIN_TOPIC_METADATA_V2);
    cluster2.addChild(EXECUTION_IDS);
    cluster2.addChild(PARENT_OFFLINE_PUSHES);
    cluster2.addChild(ROUTERS);
    cluster2.addChild(STORES);
    cluster2.addChild(STORE_GRAVEYARD);
    return root;
  }

  private void testTreeAsserts(TreeNode root) {
    Assert.assertEquals(root.getName(), BASE_PATH);
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
          Assert.assertEquals(child.getChildren().size(), 6);
          testContainsChildAsserts(child);
          for (TreeNode grandchild: child.getChildren().values()) {
            if (grandchild.getName().equals(ADMIN_TOPIC_METADATA_V2)) {
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

  private void testContainsChildAsserts(TreeNode child) {
    Assert.assertTrue(child.containsChild(ADMIN_TOPIC_METADATA_V2));
    Assert.assertTrue(child.containsChild(EXECUTION_IDS));
    Assert.assertTrue(child.containsChild(PARENT_OFFLINE_PUSHES));
    Assert.assertTrue(child.containsChild(ROUTERS));
    Assert.assertTrue(child.containsChild(STORES));
    Assert.assertTrue(child.containsChild(STORE_GRAVEYARD));
  }

  private List<String> getPaths() {
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
