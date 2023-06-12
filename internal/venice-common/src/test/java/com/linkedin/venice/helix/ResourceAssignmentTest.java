package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.ResourceAssignment.ResourceAssignmentChanges;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  public void testUpdateResourceAssignment() {
    String resource = "test";
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resource, 1);
    newResourceAssignment.setPartitionAssignment(resource, partitionAssignment);
    resourceAssignment.updateResourceAssignment(newResourceAssignment);
    Assert.assertEquals(resourceAssignment.getPartitionAssignment(resource), partitionAssignment);
    Assert.assertEquals(resourceAssignment.getAssignedResources(), newResourceAssignment.getAssignedResources());
  }

  @Test
  public void testUpdateResourceAssignmentGenerateChange() {
    // init 2 resource assignments
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();

    for (String resource: new String[] { "1", "2", "3", "4" }) {
      resourceAssignment.setPartitionAssignment(resource, getDefaultPartitionAssignment(resource, HelixState.STANDBY));
    }

    // resource 1 is the same in both resource assignments
    newResourceAssignment.setPartitionAssignment("1", getDefaultPartitionAssignment("1", HelixState.STANDBY));

    // resource 2 has different Helix state in new resource assignment
    newResourceAssignment.setPartitionAssignment("2", getDefaultPartitionAssignment("2", HelixState.ERROR));

    // resource 3 has different partitions in new resource assignment
    PartitionAssignment partitionAssignment = new PartitionAssignment("3", 2);
    partitionAssignment.addPartition(getDefaultPartition(0, 1, HelixState.STANDBY));
    partitionAssignment.addPartition(getDefaultPartition(1, 1, HelixState.STANDBY));
    newResourceAssignment.setPartitionAssignment("3", partitionAssignment);

    // resource 4 is removed in new assignment and resource 5 is a new assignment
    newResourceAssignment.setPartitionAssignment("5", getDefaultPartitionAssignment("5", HelixState.STANDBY));

    ResourceAssignmentChanges changes = resourceAssignment.updateResourceAssignment(newResourceAssignment);
    Set<String> deletedResources = changes.deletedResources;
    Set<String> updatedResources = changes.updatedResources;

    Assert.assertEquals(deletedResources.size(), 1);
    Assert.assertTrue(deletedResources.contains("4"));

    Assert.assertEquals(updatedResources.size(), 3);
    Assert.assertTrue(updatedResources.containsAll(Stream.of("2", "3", "5").collect(Collectors.toSet())));
  }

  @Test
  public void testCompareAndGetDeletedResources() {
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();

    String[] resources = new String[] { "1", "2", "3" };
    String[] newResources = new String[] { "4", "2", "5" };

    for (String resource: resources) {
      resourceAssignment.setPartitionAssignment(resource, new PartitionAssignment(resource, 1));
    }
    for (String newResource: newResources) {
      newResourceAssignment.setPartitionAssignment(newResource, new PartitionAssignment(newResource, 1));
    }
    Set<String> deletedResources = resourceAssignment.compareAndGetDeletedResources(newResourceAssignment);
    Assert.assertEquals(deletedResources.size(), 2);
    Assert.assertTrue(deletedResources.contains("1"));
    Assert.assertTrue(deletedResources.contains("3"));
  }

  @Test
  public void testUpdateResourceAssignmentNoChange() {
    // init 2 resource assignments
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();
    for (String resource: new String[] { "1", "2" }) {
      resourceAssignment.setPartitionAssignment(resource, getDefaultPartitionAssignment(resource, HelixState.STANDBY));
      newResourceAssignment
          .setPartitionAssignment(resource, getDefaultPartitionAssignment(resource, HelixState.STANDBY));
    }
    ResourceAssignmentChanges changes = resourceAssignment.updateResourceAssignment(newResourceAssignment);
    Set<String> deletedResources = changes.deletedResources;
    Set<String> updatedResources = changes.updatedResources;
    Assert.assertEquals(deletedResources.size(), 0);
    Assert.assertEquals(updatedResources.size(), 0);
  }

  private PartitionAssignment getDefaultPartitionAssignment(String topicName, HelixState helixState) {
    Partition partition = getDefaultPartition(0, 1, helixState);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topicName, 1);
    partitionAssignment.addPartition(partition);

    return partitionAssignment;
  }

  private Partition getDefaultPartition(int partitionId, int replicaNum, HelixState helixState) {
    EnumMap<HelixState, List<Instance>> stateToInstancesMap = new EnumMap<>(HelixState.class);
    for (int i = 0; i < replicaNum; i++) {
      stateToInstancesMap.put(helixState, Collections.singletonList(new Instance(String.valueOf(i), "localhost", i)));
    }

    return new Partition(partitionId, stateToInstancesMap, new EnumMap<>(ExecutionStatus.class));
  }

}
