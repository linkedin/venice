package com.linkedin.venice.helix;

import com.linkedin.venice.meta.PartitionAssignment;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ResourceAssignmentTest {

  @Test
  public void testAddAndGetPartitionAssignment() {
    String resource = "test";
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resource, 1);
    resourceAssignment.setPartitionAssignment(resource, partitionAssignment);
    Assert.assertTrue(resourceAssignment.containsResource(resource));
    Assert.assertEquals(resourceAssignment.getPartitionAssignment(resource), partitionAssignment);
  }

  @Test
  public void testRefreshAssignment() {
    String resource = "test";
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resource, 1);
    newResourceAssignment.setPartitionAssignment(resource, partitionAssignment);
    resourceAssignment.refreshAssignment(newResourceAssignment);
    Assert.assertEquals(resourceAssignment.getPartitionAssignment(resource), partitionAssignment);
    Assert.assertEquals(resourceAssignment.getAssignedResources(), newResourceAssignment.getAssignedResources());
  }

  @Test
  public void testCompareAndGetDeletedResources() {
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();

    String[] resources = new String[]{"1", "2", "3"};
    String[] newResources = new String[]{"4", "2", "5"};

    for (String resource : resources) {
      resourceAssignment.setPartitionAssignment(resource, new PartitionAssignment(resource, 1));
    }
    for (String newResource : newResources) {
      newResourceAssignment.setPartitionAssignment(newResource, new PartitionAssignment(newResource, 1));
    }
    Set<String> deletedResources = resourceAssignment.compareAndGetDeletedResources(newResourceAssignment);
    Assert.assertEquals(deletedResources.size(), 2);
    Assert.assertTrue(deletedResources.contains("1"));
    Assert.assertTrue(deletedResources.contains("3"));
  }
}
