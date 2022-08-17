package com.linkedin.venice.meta;

import java.util.Set;


public interface LiveInstanceChangedListener {
  void handleNewInstances(Set<Instance> newInstances);

  void handleDeletedInstances(Set<Instance> deletedInstances);

}
