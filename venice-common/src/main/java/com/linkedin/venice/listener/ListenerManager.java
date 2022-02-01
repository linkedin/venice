package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class provides the unified way to manager venice listener.
 *
 * @param <T> T should be a type of listener
 */
public class ListenerManager<T> {
  private static final Logger logger = LogManager.getLogger(ListenerManager.class);

  private final ExecutorService executor;
  private final Map<String, Set<T>> keyToListenersMap = new VeniceConcurrentHashMap<>();

  public ListenerManager() {
    //TODO maybe we can share the thread pool with other use-cases.
    executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("Venice-controller"));
  }

  public synchronized void subscribe(String key, T listener) {
    keyToListenersMap.putIfAbsent(key, new HashSet<>()).add(listener);
  }

  public synchronized void unsubscribe(String key, T listener) {
    Set<T> listeners = keyToListenersMap.get(key);
    if (listeners == null) {
      logger.warn("No listeners exist for the key {}", key);
      return;
    }
    if (!listeners.remove(listener)) {
      logger.warn("Listener {} not found for the key {}", listener, key);
      return;
    }
    if (listeners.isEmpty()) {
      keyToListenersMap.remove(key);
    }
  }

  /**
   * Trigger notification and execute the given handler.
   *
   * @param key
   * @param handler The function really handle the event. It accepts listener and call the corresponding handle method
   *                of this listener.
   */
  public synchronized void trigger(String key, Consumer<T> handler) {
    trigger(keyToListenersMap.get(key), handler);
    trigger(keyToListenersMap.get(Utils.WILDCARD_MATCH_ANY), handler);
  }

  private void trigger(Set<T> listeners, Consumer<T> handler) {
    if (listeners != null) {
      listeners.forEach(listener -> executor.execute(() -> {
        try {
          handler.accept(listener);
        } catch (VeniceException e) {
          logger.error("Unexpected exception thrown from listener", e);
        }
      }));
    }
  }

  // For testing-only
  Map<String, Set<T>> getKeyToListenersMap() {
    return keyToListenersMap;
  }
}
