package com.linkedin.venice.pubsub.listener;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.pubsub.listener.StoreChangeTasks.StoreChangeEventType;
import com.linkedin.venice.pubsub.listener.StoreChangeTasks.StoreCreatedTask;
import com.linkedin.venice.pubsub.listener.StoreChangeTasks.StoreDeletedTask;
import com.linkedin.venice.pubsub.listener.StoreChangeTasks.VersionAddedTask;
import com.linkedin.venice.pubsub.listener.StoreChangeTasks.VersionDeletedTask;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.LogContext;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Thread-safe notifier for store metadata changes in PubSub clients.
 *
 * <p>This notifier handles store data change events (creation, deletion, version changes) and
 * propagates them to registered PubSub adapters by executing their registered tasks asynchronously.
 *
 * <p><b>Key Features:</b>
 * <ul>
 *   <li>Event-specific tasks: Adapters register different tasks for different event types</li>
 *   <li>Rich context: Tasks receive full Store objects and version numbers</li>
 *   <li>Async execution: Tasks run in a thread pool to avoid blocking</li>
 *   <li>Failure isolation: Exceptions in one task don't affect others</li>
 *   <li>Minimal state: Only tracks version sets for change detection</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * StoreChangeNotifier notifier = new StoreChangeNotifier(
 *     4, // thread pool size
 *     logContext,
 *     VeniceComponent.SERVER
 * );
 *
 * StoreChangeTasks tasks = StoreChangeTasks.builder()
 *     .onVersionAdded((store, version) -> handleNewVersion(store, version))
 *     .build();
 *
 * String taskId = notifier.registerTasks("MyAdapter", tasks);
 *
 * // Later, when closing
 * notifier.unregisterTasks(taskId);
 * notifier.close();
 * }</pre>
 *
 * <p><b>Thread Safety:</b> All public methods are thread-safe and can be called concurrently.
 */
@ThreadSafe
public class StoreChangeNotifier implements StoreDataChangedListener, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(StoreChangeNotifier.class);
  private static final int DEFAULT_THREAD_POOL_SIZE = 1;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

  private final ExecutorService notificationExecutor;
  private final ConcurrentHashMap<String, StoreChangeTasks> taskRegistry;
  private final Object2ObjectMap<String, IntSet> storeVersionSets;
  private final AtomicInteger taskIdSuffixCounter;
  private final AtomicBoolean closed;
  private final VeniceComponent veniceComponent;

  public StoreChangeNotifier(LogContext logContext, VeniceComponent veniceComponent) {
    this(DEFAULT_THREAD_POOL_SIZE, logContext, veniceComponent);
  }

  public StoreChangeNotifier(int threadPoolSize, LogContext logContext, VeniceComponent veniceComponent) {
    if (threadPoolSize <= 0) {
      throw new IllegalArgumentException("Thread pool size must be positive, got: " + threadPoolSize);
    }
    if (logContext == null) {
      throw new IllegalArgumentException("LogContext cannot be null");
    }
    if (veniceComponent == null) {
      throw new IllegalArgumentException("VeniceComponent cannot be null");
    }

    this.veniceComponent = veniceComponent;
    this.notificationExecutor =
        Executors.newFixedThreadPool(threadPoolSize, new DaemonThreadFactory("pubsub-client-notifier", logContext));
    this.taskRegistry = new ConcurrentHashMap<>();
    this.storeVersionSets = Object2ObjectMaps.synchronize(new Object2ObjectOpenHashMap<>());
    this.taskIdSuffixCounter = new AtomicInteger(0);
    this.closed = new AtomicBoolean(false);

    LOGGER.info(
        "StoreChangeNotifier initialized for {} with {} notification threads",
        veniceComponent.getName(),
        threadPoolSize);
  }

  /**
   * Registers tasks for store change events with a client-provided ID prefix.
   *
   * <p>The notifier appends a unique suffix to ensure uniqueness. Callers should retain
   * the returned task ID for later unregistration.
   *
   * @param clientId client-provided ID prefix (e.g., adapter class name)
   * @param tasks the tasks to execute on store events
   * @return unique task ID in format "{clientId}-{uniqueSuffix}" for later unregistration
   * @throws IllegalArgumentException if clientId is null/empty or tasks is null
   * @throws IllegalStateException if notifier has been closed
   */
  public String registerTasks(String clientId, StoreChangeTasks tasks) {
    if (clientId == null || clientId.isEmpty()) {
      throw new IllegalArgumentException("Client ID cannot be null or empty");
    }
    if (tasks == null) {
      throw new IllegalArgumentException("Tasks cannot be null");
    }
    if (closed.get()) {
      throw new IllegalStateException("Notifier has been closed");
    }

    String uniqueTaskId = generateUniqueTaskId(clientId);
    taskRegistry.put(uniqueTaskId, tasks);

    LOGGER.info("Registered tasks with ID: {} (client: {})", uniqueTaskId, clientId);
    return uniqueTaskId;
  }

  /**
   * Unregisters previously registered tasks.
   *
   * @param taskId the unique task ID returned from {@link #registerTasks}
   * @return true if tasks were found and removed, false otherwise
   */
  public boolean unregisterTasks(String taskId) {
    if (taskId == null) {
      return false;
    }

    StoreChangeTasks removed = taskRegistry.remove(taskId);
    if (removed != null) {
      LOGGER.info("Unregistered tasks with ID: {}", taskId);
      return true;
    }
    return false;
  }

  /**
   * Returns the number of currently registered task sets.
   */
  public int getRegisteredTaskCount() {
    return taskRegistry.size();
  }

  @Override
  public void handleStoreCreated(Store store) {
    if (store == null) {
      LOGGER.warn("Received null store in handleStoreCreated");
      return;
    }

    String storeName = store.getName();
    LOGGER.info("Store created: {}", storeName);

    // Initialize tracking state for the new store
    storeVersionSets.put(storeName, new IntOpenHashSet(store.getVersionNumbers()));

    notifyTasksForStoreCreated(store);
  }

  @Override
  public void handleStoreDeleted(Store store) {
    if (store == null) {
      LOGGER.warn("Received null store in handleStoreDeleted");
      return;
    }

    String storeName = store.getName();
    LOGGER.info("Store deleted: {}", storeName);

    // Notify tasks BEFORE cleaning up state (tasks may need current state)
    notifyTasksForStoreDeleted(store);

    storeVersionSets.remove(storeName);
  }

  @Override
  public void handleStoreChanged(Store store) {
    if (store == null) {
      LOGGER.warn("Received null store in handleStoreChanged");
      return;
    }

    String storeName = store.getName();

    // Detect what changed by comparing with previous state
    IntSet previousVersions = storeVersionSets.get(storeName);
    IntSet currentVersions = new IntOpenHashSet(store.getVersionNumbers());

    if (previousVersions != null) {
      // Check for version deletions
      IntSet deletedVersions = new IntOpenHashSet(previousVersions);
      deletedVersions.removeAll(currentVersions);

      for (int deletedVersion: deletedVersions) {
        LOGGER.info("Store {} version deleted: {}", storeName, deletedVersion);
        notifyTasksForVersionDeleted(store, deletedVersion);
      }

      // Check for new versions added
      IntSet addedVersions = new IntOpenHashSet(currentVersions);
      addedVersions.removeAll(previousVersions);

      for (int addedVersion: addedVersions) {
        LOGGER.info("Store {} version added: {}", storeName, addedVersion);
        notifyTasksForVersionAdded(store, addedVersion);
      }
    }

    // Update tracking state
    storeVersionSets.put(storeName, currentVersions);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      LOGGER.info("Closing StoreChangeNotifier for {}...", veniceComponent.getName());

      notificationExecutor.shutdown();
      try {
        if (!notificationExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          LOGGER.warn(
              "Notification executor did not terminate within {} seconds, forcing shutdown",
              SHUTDOWN_TIMEOUT_SECONDS);
          notificationExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while waiting for notification executor to terminate", e);
        notificationExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }

      int taskCount = taskRegistry.size();
      taskRegistry.clear();

      storeVersionSets.clear();

      LOGGER.info(
          "StoreChangeNotifier for {} closed. Cleared {} task registrations",
          veniceComponent.getName(),
          taskCount);
    }
  }

  private void notifyTasksForStoreCreated(Store store) {
    int taskCount = 0;
    for (Map.Entry<String, StoreChangeTasks> entry: taskRegistry.entrySet()) {
      String taskId = entry.getKey();
      StoreCreatedTask task = entry.getValue().getOnStoreCreated();

      if (task != null) {
        taskCount++;
        submitTask(taskId, () -> task.execute(store), StoreChangeEventType.STORE_CREATED, store.getName());
      }
    }

    if (taskCount > 0) {
      LOGGER.info("Submitted {} tasks for STORE_CREATED event on store: {}", taskCount, store.getName());
    }
  }

  private void notifyTasksForStoreDeleted(Store store) {
    int taskCount = 0;
    for (Map.Entry<String, StoreChangeTasks> entry: taskRegistry.entrySet()) {
      String taskId = entry.getKey();
      StoreDeletedTask task = entry.getValue().getOnStoreDeleted();

      if (task != null) {
        taskCount++;
        submitTask(taskId, () -> task.execute(store), StoreChangeEventType.STORE_DELETED, store.getName());
      }
    }

    if (taskCount > 0) {
      LOGGER.info("Submitted {} tasks for STORE_DELETED event on store: {}", taskCount, store.getName());
    }
  }

  private void notifyTasksForVersionAdded(Store store, int versionNumber) {
    int taskCount = 0;
    for (Map.Entry<String, StoreChangeTasks> entry: taskRegistry.entrySet()) {
      String taskId = entry.getKey();
      VersionAddedTask task = entry.getValue().getOnVersionAdded();

      if (task != null) {
        taskCount++;
        submitTask(
            taskId,
            () -> task.execute(store, versionNumber),
            StoreChangeEventType.VERSION_ADDED,
            store.getName() + " (v" + versionNumber + ")");
      }
    }

    if (taskCount > 0) {
      LOGGER.info(
          "Submitted {} tasks for VERSION_ADDED event on store: {}, version: {}",
          taskCount,
          store.getName(),
          versionNumber);
    }
  }

  private void notifyTasksForVersionDeleted(Store store, int versionNumber) {
    int taskCount = 0;
    for (Map.Entry<String, StoreChangeTasks> entry: taskRegistry.entrySet()) {
      String taskId = entry.getKey();
      VersionDeletedTask task = entry.getValue().getOnVersionDeleted();

      if (task != null) {
        taskCount++;
        submitTask(
            taskId,
            () -> task.execute(store, versionNumber),
            StoreChangeEventType.VERSION_DELETED,
            store.getName() + " (v" + versionNumber + ")");
      }
    }

    if (taskCount > 0) {
      LOGGER.info(
          "Submitted {} tasks for VERSION_DELETED event on store: {}, version: {}",
          taskCount,
          store.getName(),
          versionNumber);
    }
  }

  private void submitTask(String taskId, Runnable taskRunnable, StoreChangeEventType eventType, String context) {
    if (closed.get()) {
      LOGGER.warn("Attempted to submit task after notifier was closed");
      return;
    }

    try {
      notificationExecutor.submit(() -> {
        try {
          LOGGER.info("Executing task {} for {} event: {}", taskId, eventType.name(), context);
          taskRunnable.run();
        } catch (Exception e) {
          // Catch all exceptions to prevent one task failure from affecting others
          LOGGER.error("Error executing task {} for {} event: {}", taskId, eventType.name(), context, e);
        }
      });
    } catch (RejectedExecutionException e) {
      LOGGER.error("Failed to submit task {} for {} event: {}", taskId, eventType.name(), context, e);
    }
  }

  private String generateUniqueTaskId(String clientId) {
    return clientId + "-" + taskIdSuffixCounter.incrementAndGet();
  }
}
