package com.linkedin.venice.init;

import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * This class provides the basic framework necessary to create new schema system stores and register previously unknown
 * schemas for schema system stores. This framework will orchestrate the various {@link AbstractSystemSchemaInitializer}
 * objects. The scope of an object of this class is all the schema stores for the system stores that the component
 * writes data to
 */
public abstract class AbstractSystemSchemaInitializationRoutine {
  /**
   * @return a list of all the {@link AbstractSystemSchemaInitializer}. One for each schema system store.
   */
  public abstract List<AbstractSystemSchemaInitializer> getSystemSchemaInitializers();

  public enum InitializationState {
    SUCCEEDED, FAILED, INTERRUPTED
  }

  public InitializationState execute() {
    // First, get the system schema initializers that need to be executed
    List<AbstractSystemSchemaInitializer> systemSchemaInitializerList = getSystemSchemaInitializers();

    if (systemSchemaInitializerList == null || systemSchemaInitializerList.isEmpty()) {
      return InitializationState.SUCCEEDED;
    }

    // Then, execute all the system schema initializers
    for (AbstractSystemSchemaInitializer systemSchemaInitializer: systemSchemaInitializerList) {
      systemSchemaInitializer.execute();
    }

    // Wait for the required schemas to propagate to the local fabrics
    Set<AbstractSystemSchemaInitializer> pendingSchemaInitializations = new HashSet<>(systemSchemaInitializerList);
    long startTime = System.currentTimeMillis();
    while (LatencyUtils.getLatencyInMS(startTime) < 10 * Time.MS_PER_MINUTE) {
      Set<AbstractSystemSchemaInitializer> completedSystemSchemaInitialization = new HashSet<>();
      for (AbstractSystemSchemaInitializer systemSchemaInitializer: pendingSchemaInitializations) {
        if (systemSchemaInitializer.isSchemaInitialized()) {
          completedSystemSchemaInitialization.add(systemSchemaInitializer);
        }
      }

      pendingSchemaInitializations.removeAll(completedSystemSchemaInitialization);
      if (pendingSchemaInitializations.isEmpty()) {
        return InitializationState.SUCCEEDED;
      }

      if (!Utils.sleep(10 * Time.MS_PER_SECOND)) {
        return InitializationState.INTERRUPTED;
      }
    }

    return InitializationState.FAILED;
  }
}
