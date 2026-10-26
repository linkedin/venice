package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import java.util.Optional;
import java.util.function.BiConsumer;


/**
 * Supplies application execution context without coupling the client to a particular context implementation.
 */
@FunctionalInterface
public interface ExecutionContextProvider {
  ExecutionContextProvider NO_OP = () -> Runnable::run;

  /**
   * Capture the calling thread's context before crossing an asynchronous boundary.
   * The returned snapshot must support concurrent and repeated use.
   */
  CapturedContext capture();

  @FunctionalInterface
  interface CapturedContext {
    /**
     * Run the task synchronously, exactly once, under the captured context. Implementations must restore the
     * executing thread's previous context in a finally block, including when the task throws. Task exceptions
     * must propagate without retrying the task or falling back to execution without context.
     */
    void run(Runnable task);

    default Runnable wrap(Runnable task) {
      return () -> run(task);
    }

    default <T, U> BiConsumer<T, U> wrap(BiConsumer<T, U> callback) {
      return (value, exception) -> run(() -> callback.accept(value, exception));
    }

    default <K, V> StreamingCallback<K, V> wrap(StreamingCallback<K, V> callback) {
      return new StreamingCallback<K, V>() {
        @Override
        public void onRecordReceived(K key, V value) {
          run(() -> callback.onRecordReceived(key, value));
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          run(() -> callback.onCompletion(exception));
        }
      };
    }
  }
}
