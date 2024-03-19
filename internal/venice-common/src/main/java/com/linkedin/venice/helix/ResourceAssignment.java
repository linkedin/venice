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
 *
 * N.B.: This class seems to have thread safety issues. The {@link #updateResourceAssignment(ResourceAssignment)}
 * function swaps the {@link #resourceToAssignmentsMap} to another reference, and the two callers of that function wrap
 * its invocation within locks. However, there are many other code paths which perform lock-free accesses to the same
 * map across two functions: {@link #containsResource(String)} and {@link #getPartitionAssignment(String)}. These other
 * code paths first check if the resource is contained, then if it is, they get it. But there is no guarantee that the
 * reference to the map is the same, since {@link #updateResourceAssignment(ResourceAssignment)} could have been called
 * in between. Ideally, we would eliminate this "contains, then get" pattern entirely, and instead rely on a single
 * invocation to get the resource (which should be allowed to return null, rather than throw when absent); then the
 * caller can use a null check as the equivalent of today's contains check, which would make the operation atomic and
 * thus impossible to interleave with {@link #updateResourceAssignment(ResourceAssignment)}. TODO: refactor this.
 */
public class ResourceAssignment {
  private static final Logger LOGGER = LogManager.getLogger(ResourceAssignment.class);

  private volatile Map<String, PartitionAssignment> resourceToAssignmentsMap = new HashMap<>();

  /**
   * TODO: Rename this "getPartitionAssignmentOrThrow", and create an equivalent which returns null when absent.
   */
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

  /**
   * TODO: Delete this function entirely, to avoid the "contains, then get" anti-pattern.
   */
  @Deprecated
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
    Set<String> newResources = compareAndGetNewResources(newAssignment);

    this.resourceToAssignmentsMap = newAssignment.resourceToAssignmentsMap;
    return new ResourceAssignmentChanges(deletedResources, updatedResources, newResources);
  }

  Set<String> compareAndGetNewResources(ResourceAssignment newAssignment) {
    return newAssignment.getAssignedResources()
        .stream()
        .filter(newResource -> !containsResource(newResource))
        .collect(Collectors.toSet());
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
    Set<String> newResources;

    ResourceAssignmentChanges(Set<String> deletedResources, Set<String> updatedResources, Set<String> newResources) {
      this.deletedResources = deletedResources;
      this.updatedResources = updatedResources;
      this.newResources = newResources;
    }

    public Set<String> getDeletedResource() {
      return deletedResources;
    }

    public Set<String> getUpdatedResources() {
      return updatedResources;
    }

    public Set<String> getNewResources() {
      return newResources;
    }
  }
}
