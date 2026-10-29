package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import java.util.Set;


/**
 * Handles a successful store update consumed by a parent controller.
 *
 * <p><b>Implementation contract</b> — implementations must respect the following semantics:</p>
 * <ul>
 *   <li><b>Concurrency:</b> {@link #handleStoreUpdate} may be invoked concurrently for different stores. Any shared
 *       state an implementation touches must be thread-safe.</li>
 *   <li><b>At-least-once / idempotency:</b> a handler may be invoked more than once for the same admin operation when
 *       checkpointing is retried, so external side effects must be idempotent or otherwise tolerate duplicate
 *       invocations.</li>
 *   <li><b>No reentrant controller operations:</b> the handler runs synchronously on the admin-consumption path while
 *       the originating {@code updateStore} caller still holds the per-store admin-message lock and waits for this
 *       checkpoint. A handler that synchronously issues another controller admin operation for the same store would
 *       block on that lock and deadlock until timeout. Handlers must not perform reentrant/blocking controller
 *       operations; offload such work asynchronously instead.</li>
 * </ul>
 */
@FunctionalInterface
public interface StoreUpdateHandler {
  StoreUpdateHandler NO_OP = (clusterName, store, updatedConfigs) -> {};

  /**
   * @param clusterName the cluster containing the updated store
   * @param store a read-only snapshot of the final store state
   * @param updatedConfigs the immutable set of config keys copied from the durable UPDATE_STORE message; the set is
   *        stable across retries of the same admin operation. An empty set means the update was issued with
   *        {@code --replicate-all-configs}, in which case the durable message carries no per-config list at all and
   *        every config on the message was applied. Handlers must therefore treat an empty set as "the whole store
   *        was replicated" and fall back to {@code store}, never as "nothing changed".
   */
  void handleStoreUpdate(String clusterName, Store store, Set<String> updatedConfigs);
}
