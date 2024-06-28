package com.linkedin.venice;

import com.linkedin.venice.ZkCopier.TreeNode;
import java.util.ArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestTreeNode {
  @Test
  public void testTreeNode() {
    // testing listToTree()
    String basePath = "/venice-parent";
    TreeNode node = ZkCopier.listToTree(getPaths(), basePath);
    testListToTree(node);

    // testing TreeNode class methods
    TreeNode root = new TreeNode(basePath);
    testTreeNodeMethods(root);

    // testing treeToList()
    ArrayList<String> list = ZkCopier.treeToList(root);
    testTreeToList(list);
  }

  public void testListToTree(TreeNode root) {
    Assert.assertEquals(root.getChildren().size(), 3);
    Assert.assertTrue(root.contains("storeConfigs"));
    Assert.assertTrue(root.contains("cluster1"));
    Assert.assertTrue(root.contains("cluster2"));
    for (TreeNode child: root.getChildren().values()) {
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
          for (TreeNode grandchild: child.getChildren().values()) {
            Assert.assertEquals(grandchild.getPath(), "/venice-parent/cluster1/" + grandchild.getVal());
            if (grandchild.getVal().equals("adminTopicMetadata")) {
              Assert.assertEquals(grandchild.getChildren().size(), 2);
              for (TreeNode greatgrandchild: grandchild.getChildren().values()) {
                Assert.assertEquals(
                    greatgrandchild.getPath(),
                    "/venice-parent/cluster1/adminTopicMetadata/" + greatgrandchild.getVal());
                if (greatgrandchild.getVal().equals("file2")) {
                  Assert.assertEquals(greatgrandchild.getChildren().size(), 1);
                  for (TreeNode greatgreatgrandchild: greatgrandchild.getChildren().values()) {
                    Assert.assertEquals(
                        greatgreatgrandchild.getPath(),
                        "/venice-parent/cluster1/adminTopicMetadata/file2/" + greatgreatgrandchild.getVal());
                    Assert.assertEquals(greatgreatgrandchild.getChildren().size(), 0);
                  }
                } else {
                  Assert.assertEquals(greatgrandchild.getChildren().size(), 0);
                }
              }
            }
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
          for (TreeNode grandchild: child.getChildren().values()) {
            Assert.assertEquals(grandchild.getPath(), "/venice-parent/cluster2/" + grandchild.getVal());
            Assert.assertEquals(grandchild.getChildren().size(), 0);
          }
          break;
      }
    }
  }

  public void testTreeNodeMethods(TreeNode root) {
    Assert.assertEquals(root.getVal(), "/venice-parent");
    Assert.assertEquals(root.getPath(), "/venice-parent");
    Assert.assertEquals(root.getChildren().size(), 0);
    root.addChild("storeConfigs");
    root.addChild("cluster1");
    root.addChild("cluster2");
    Assert.assertEquals(root.getChildren().size(), 3);
    Assert.assertTrue(root.contains("storeConfigs"));
    Assert.assertTrue(root.contains("cluster1"));
    Assert.assertTrue(root.contains("cluster2"));
    for (TreeNode child: root.getChildren().values()) {
      switch (child.getVal()) {
        case "storeConfigs":
          Assert.assertEquals(child.getPath(), "/venice-parent/storeConfigs");
          Assert.assertEquals(child.getChildren().size(), 0);
          break;
        case "cluster1":
          Assert.assertEquals(child.getPath(), "/venice-parent/cluster1");
          Assert.assertEquals(child.getChildren().size(), 0);
          child.addChild("storeConfigs");
          child.addChild("adminTopicMetadata");
          child.addChild("executionids");
          child.addChild("ParentOfflinePushes");
          child.addChild("routers");
          child.addChild("StoreGraveyard");
          child.addChild("Stores");
          Assert.assertEquals(child.getChildren().size(), 7);
          Assert.assertTrue(child.contains("storeConfigs"));
          child.deleteChild("storeConfigs");
          Assert.assertEquals(child.getChildren().size(), 6);
          Assert.assertFalse(child.contains("storeConfigs"));
          Assert.assertTrue(child.contains("adminTopicMetadata"));
          Assert.assertTrue(child.contains("executionids"));
          Assert.assertTrue(child.contains("ParentOfflinePushes"));
          Assert.assertTrue(child.contains("routers"));
          Assert.assertTrue(child.contains("StoreGraveyard"));
          Assert.assertTrue(child.contains("Stores"));
          for (TreeNode grandchild: child.getChildren().values()) {
            Assert.assertEquals(grandchild.getPath(), "/venice-parent/cluster1/" + grandchild.getVal());
            Assert.assertEquals(grandchild.getChildren().size(), 0);
            if (grandchild.getVal().equals("adminTopicMetadata")) {
              grandchild.addChild("file1");
              grandchild.addChild("file2");
              Assert.assertEquals(grandchild.getChildren().size(), 2);
              for (TreeNode greatgrandchild: grandchild.getChildren().values()) {
                Assert.assertEquals(
                    greatgrandchild.getPath(),
                    "/venice-parent/cluster1/adminTopicMetadata/" + greatgrandchild.getVal());
                if (greatgrandchild.getVal().equals("file2")) {
                  greatgrandchild.addChild("file3");
                  Assert.assertEquals(greatgrandchild.getChildren().size(), 1);
                  for (TreeNode greatgreatgrandchild: greatgrandchild.getChildren().values()) {
                    Assert.assertEquals(
                        greatgreatgrandchild.getPath(),
                        "/venice-parent/cluster1/adminTopicMetadata/file2/" + greatgreatgrandchild.getVal());
                    Assert.assertEquals(greatgreatgrandchild.getChildren().size(), 0);
                  }
                } else {
                  Assert.assertEquals(greatgrandchild.getChildren().size(), 0);
                }
              }
            }
          }
          break;
        case "cluster2":
          Assert.assertEquals(child.getPath(), "/venice-parent/cluster2");
          Assert.assertEquals(child.getChildren().size(), 0);
          child.addChild("storeConfigs");
          child.addChild("adminTopicMetadata");
          child.addChild("executionids");
          child.addChild("ParentOfflinePushes");
          child.addChild("routers");
          child.addChild("StoreGraveyard");
          child.addChild("Stores");
          Assert.assertEquals(child.getChildren().size(), 7);
          Assert.assertTrue(child.contains("storeConfigs"));
          child.deleteChild("storeConfigs");
          Assert.assertEquals(child.getChildren().size(), 6);
          Assert.assertFalse(child.contains("storeConfigs"));
          Assert.assertTrue(child.contains("adminTopicMetadata"));
          Assert.assertTrue(child.contains("executionids"));
          Assert.assertTrue(child.contains("ParentOfflinePushes"));
          Assert.assertTrue(child.contains("routers"));
          Assert.assertTrue(child.contains("StoreGraveyard"));
          Assert.assertTrue(child.contains("Stores"));
          for (TreeNode grandchild: child.getChildren().values()) {
            Assert.assertEquals(grandchild.getPath(), "/venice-parent/cluster2/" + grandchild.getVal());
            Assert.assertEquals(grandchild.getChildren().size(), 0);
          }
          break;
      }
    }
  }

  public void testTreeToList(ArrayList<String> list) {
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

  public ArrayList<String> getPaths() {
    ArrayList<String> paths = new ArrayList<>();
    paths.add("/venice-parent/storeConfigs");
    paths.add("/venice-parent/cluster1");
    paths.add("/venice-parent/cluster1/adminTopicMetadata");
    paths.add("/venice-parent/cluster1/adminTopicMetadata/file1");
    paths.add("/venice-parent/cluster1/adminTopicMetadata/file2/file3"); // tests both /file2 and /file/file3
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
