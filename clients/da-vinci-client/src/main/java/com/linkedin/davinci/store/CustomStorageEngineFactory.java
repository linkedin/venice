package com.linkedin.davinci.store;

public interface CustomStorageEngineFactory<E extends CustomStorageEngine> {
  E createEngine();

  void setCurrentVersion(E engine);

  void retireVersion(E engine);

  void removeCustomStorageEngine(E engine);

  void close();
}
