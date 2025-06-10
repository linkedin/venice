package com.linkedin.davinci.store;

public class StorageEngineAccessor {
  /**
   * Do not move out of the test module!
   *
   * This is a backdoor to extirpate the storage engine wrapped inside a {@link DelegatingStorageEngine}.
   */
  public static <SE extends StorageEngine> SE getInnerStorageEngine(StorageEngine storageEngine) {
    if (!(storageEngine instanceof DelegatingStorageEngine)) {
      throw new IllegalArgumentException(
          "Passed in param must be an instance of " + DelegatingStorageEngine.class.getSimpleName());
    }
    DelegatingStorageEngine delegatingStorageEngine = (DelegatingStorageEngine) storageEngine;
    StorageEngine innerStorageEngine = delegatingStorageEngine.getDelegate();
    return (SE) innerStorageEngine;
  }
}
