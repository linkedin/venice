package com.linkedin.venice.listener;

import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  private final ConcurrentMap<String, Set<T>> listenerMap;

  private final ExecutorService threadPool;

  // TODO make thread count and keepAlive time configurable.
  private final int threadCount = 1;

  private static final Logger LOGGER = LogManager.getLogger(ListenerManager.class);

  public ListenerManager() {
    listenerMap = new ConcurrentHashMap<>();
    // TODO maybe we can share the thread pool with other use-cases.
    threadPool = Executors.newFixedThreadPool(threadCount, new DaemonThreadFactory("Venice-controller"));
  }

  public synchronized void subscribe(String key, T listener) {
    Set<T> set;
    if (!listenerMap.containsKey(key)) {
      set = new HashSet<>();
      listenerMap.put(key, set);
    } else {
      set = listenerMap.get(key);
    }
    set.add(listener);
  }

  public synchronized void unsubscribe(String key, T listener) {
    if (!listenerMap.containsKey(key)) {
      LOGGER.debug("Not listeners are found for given key: {}", key);
    } else {
      listenerMap.get(key).remove(listener);
      if (listenerMap.get(key).isEmpty()) {
        listenerMap.remove(key);
      }
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
    trigger(listenerMap.get(key), handler);
    trigger(listenerMap.get(Utils.WILDCARD_MATCH_ANY), handler);
  }

  private void trigger(Set<T> listeners, Consumer<T> handler) {
    if (listeners != null) {
      listeners.forEach(listener -> threadPool.execute(() -> handler.accept(listener)));
    }
  }

  ConcurrentMap<String, Set<T>> getListenerMap() {
    return listenerMap;
  }
}
