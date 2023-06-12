package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Assignments for all of resources. This is NOT a thread-safe class. But in our use case, there is no any concurrent
 * operations to assignments. So here we only use volatile resourceToAssignmentsMap to ensure the reference is up to
 * date.
 */
public class ResourceAssignment {
  private static final Logger LOGGER = LogManager.getLogger(ResourceAssignment.class);

  private volatile Map<String, PartitionAssignment> resourceToAssignmentsMap = new HashMap<>();

  public PartitionAssignment getPartitionAssignment(String resource) {
    PartitionAssignment partitionAssignment = resourceToAssignmentsMap.get(resource);
    if (partitionAssignment == null) {
      LOGGER.trace("Resource '{}' does not exist", resource);
      // TODO: Might want to add some (configurable) retries here or higher up the stack. If the Helix spectator is out
      // of sync, this fails...
      throw new VeniceNoHelixResourceException(resource);
    }
    return partitionAssignment;
  }

  public void setPartitionAssignment(String resource, PartitionAssignment partitionAssignment) {
    resourceToAssignmentsMap.put(resource, partitionAssignment);
  }

  public Partition getPartition(String resource, int partitionId) {
    return getPartitionAssignment(resource).getPartition(partitionId);
  }

  public boolean containsResource(String resource) {
    return resourceToAssignmentsMap.containsKey(resource);
  }

  public Set<String> getAssignedResources() {
    return resourceToAssignmentsMap.keySet();
  }

  /**
   * Calculate resource assignment changes between the current one and the given one
   * and also swap the current assignment to the given one in the end
   *
   * @param newAssignment the given resource assignment
   * @return the changes between 2 assignments
   */
  public ResourceAssignmentChanges updateResourceAssignment(ResourceAssignment newAssignment) {
    Set<String> deletedResources = compareAndGetDeletedResources(newAssignment);
    Set<String> updatedResources = compareAndGetUpdatedResources(newAssignment);

    this.resourceToAssignmentsMap = newAssignment.resourceToAssignmentsMap;
    return new ResourceAssignmentChanges(deletedResources, updatedResources);
  }

  Set<String> compareAndGetDeletedResources(ResourceAssignment newAssignment) {
    return resourceToAssignmentsMap.keySet()
        .stream()
        .filter(originalResources -> !newAssignment.containsResource(originalResources))
        .collect(Collectors.toSet());
  }

  Set<String> compareAndGetUpdatedResources(ResourceAssignment newAssignment) {
    return newAssignment.getAssignedResources()
        .stream()
        .filter(
            newResource -> !containsResource(newResource)
                || !getPartitionAssignment(newResource).equals(newAssignment.getPartitionAssignment(newResource)))
        .collect(Collectors.toSet());
  }

  public static class ResourceAssignmentChanges {
    Set<String> deletedResources;
    Set<String> updatedResources;

    ResourceAssignmentChanges(Set<String> deletedResources, Set<String> updatedResources) {
      this.deletedResources = deletedResources;
      this.updatedResources = updatedResources;
    }

    public Set<String> getDeletedResource() {
      return deletedResources;
    }

    public Set<String> getUpdatedResources() {
      return updatedResources;
    }
  }
}
