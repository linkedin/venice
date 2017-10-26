package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * Assignments for all of resources. This is NOT a thread-safe class. But in our use case, there is no any concurrent
 * operations to assignments. So here we only use volatile resourceToAssignmentsMap to ensure the reference is up to
 * date.
 */
public class ResourceAssignment {
  private static final Logger logger = Logger.getLogger(ResourceAssignment.class);

  private volatile Map<String, PartitionAssignment> resourceToAssignmentsMap = new HashMap<>();

  public PartitionAssignment getPartitionAssignment(String resource) {
    checkResource(resource);
    return resourceToAssignmentsMap.get(resource);
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

  private void checkResource(String resourceName) {
    if (!resourceToAssignmentsMap.containsKey(resourceName)) {
      String errorMessage = "Resource '" + resourceName + "' does not exist";
      logger.trace(errorMessage);
      // TODO: Might want to add some (configurable) retries here or higher up the stack. If the Helix spectator is out of sync, this fails...
      throw new VeniceException(errorMessage);
    }
  }

  protected void refreshAssignment(ResourceAssignment newAssignment) {
    resourceToAssignmentsMap = newAssignment.resourceToAssignmentsMap;
  }

  protected Set<String> compareAndGetDeletedResources(ResourceAssignment newAssignment) {
    return resourceToAssignmentsMap.keySet()
        .stream()
        .filter(originalResources -> !newAssignment.containsResource(originalResources))
        .collect(Collectors.toSet());
  }
}
