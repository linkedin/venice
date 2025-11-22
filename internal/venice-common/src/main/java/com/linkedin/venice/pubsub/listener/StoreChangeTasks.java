package com.linkedin.venice.pubsub.listener;

import com.linkedin.venice.meta.Store;
import javax.annotation.concurrent.Immutable;


/**
 * Immutable collection of tasks for different store change events.
 *
 * <p>Adapters register these tasks to react to store metadata changes. Each task is optional -
 * adapters only register tasks for events they care about.
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * StoreChangeTasks tasks = StoreChangeTasks.builder()
 *     .onStoreCreated(store -> cache.initialize(store.getName()))
 *     .onVersionAdded((store, version) -> cache.invalidate(store.getName()))
 *     .onVersionDeleted((store, version) -> cache.cleanup(store.getName(), version))
 *     .onCurrentVersionChanged((store, newVersion, oldVersion) ->
 *         cache.updateCurrentVersion(store.getName(), newVersion))
 *     .onStoreDeleted(store -> cache.remove(store.getName()))
 *     .build();
 *
 * String taskId = notifier.registerTasks("MyAdapter", tasks);
 * }</pre>
 *
 * <p><b>Thread Safety:</b> This class is immutable and thread-safe. Tasks are executed
 * asynchronously, so task implementations should be thread-safe.
 */
@Immutable
public final class StoreChangeTasks {
  /**
   * Event types for store metadata changes.
   */
  public enum StoreChangeEventType {
    STORE_CREATED, STORE_DELETED, VERSION_ADDED, VERSION_DELETED, CURRENT_VERSION_CHANGED
  }

  /**
   * Task executed when a store is created.
   */
  @FunctionalInterface
  public interface StoreCreatedTask {
    void execute(Store store);
  }

  /**
   * Task executed when a store is deleted.
   */
  @FunctionalInterface
  public interface StoreDeletedTask {
    void execute(Store store);
  }

  /**
   * Task executed when a new version is added to a store.
   */
  @FunctionalInterface
  public interface VersionAddedTask {
    void execute(Store store, int addedVersionNumber);
  }

  /**
   * Task executed when a version is deleted from a store.
   */
  @FunctionalInterface
  public interface VersionDeletedTask {
    void execute(Store store, int deletedVersionNumber);
  }

  /**
   * Task executed when a store's current version changes.
   */
  @FunctionalInterface
  public interface CurrentVersionChangedTask {
    void execute(Store store, int newCurrentVersion, int previousCurrentVersion);
  }

  private final StoreCreatedTask onStoreCreated;
  private final StoreDeletedTask onStoreDeleted;
  private final VersionAddedTask onVersionAdded;
  private final VersionDeletedTask onVersionDeleted;
  private final CurrentVersionChangedTask onCurrentVersionChanged;

  private StoreChangeTasks(Builder builder) {
    this.onStoreCreated = builder.onStoreCreated;
    this.onStoreDeleted = builder.onStoreDeleted;
    this.onVersionAdded = builder.onVersionAdded;
    this.onVersionDeleted = builder.onVersionDeleted;
    this.onCurrentVersionChanged = builder.onCurrentVersionChanged;
  }

  public static Builder builder() {
    return new Builder();
  }

  public StoreCreatedTask getOnStoreCreated() {
    return onStoreCreated;
  }

  public StoreDeletedTask getOnStoreDeleted() {
    return onStoreDeleted;
  }

  public VersionAddedTask getOnVersionAdded() {
    return onVersionAdded;
  }

  public VersionDeletedTask getOnVersionDeleted() {
    return onVersionDeleted;
  }

  public CurrentVersionChangedTask getOnCurrentVersionChanged() {
    return onCurrentVersionChanged;
  }

  /**
   * Builder for constructing {@link StoreChangeTasks} instances.
   * All tasks are optional - only register tasks for events you care about.
   */
  public static class Builder {
    private StoreCreatedTask onStoreCreated;
    private StoreDeletedTask onStoreDeleted;
    private VersionAddedTask onVersionAdded;
    private VersionDeletedTask onVersionDeleted;
    private CurrentVersionChangedTask onCurrentVersionChanged;

    public Builder onStoreCreated(StoreCreatedTask task) {
      this.onStoreCreated = task;
      return this;
    }

    public Builder onStoreDeleted(StoreDeletedTask task) {
      this.onStoreDeleted = task;
      return this;
    }

    public Builder onVersionAdded(VersionAddedTask task) {
      this.onVersionAdded = task;
      return this;
    }

    public Builder onVersionDeleted(VersionDeletedTask task) {
      this.onVersionDeleted = task;
      return this;
    }

    public Builder onCurrentVersionChanged(CurrentVersionChangedTask task) {
      this.onCurrentVersionChanged = task;
      return this;
    }

    public StoreChangeTasks build() {
      return new StoreChangeTasks(this);
    }
  }
}
