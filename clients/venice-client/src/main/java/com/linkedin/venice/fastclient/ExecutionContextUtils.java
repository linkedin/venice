package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


final class ExecutionContextUtils {
  private static final Logger LOGGER = LogManager.getLogger(ExecutionContextUtils.class);

  private ExecutionContextUtils() {
  }

  static Runnable wrap(
      ExecutionContextProvider.CapturedContext context,
      Runnable task,
      Consumer<Throwable> failureHandler) {
    if (context == ExecutionContextProvider.CapturedContext.NO_OP) {
      return task;
    }
    return () -> run(context, task, failureHandler);
  }

  static <T, U> BiConsumer<T, U> wrap(
      ExecutionContextProvider.CapturedContext context,
      BiConsumer<T, U> callback,
      Consumer<Throwable> failureHandler) {
    if (context == ExecutionContextProvider.CapturedContext.NO_OP) {
      return callback;
    }
    return (value, error) -> run(context, () -> callback.accept(value, error), failureHandler);
  }

  static <K, V> StreamingCallback<K, V> guardCompletion(
      ExecutionContextProvider.CapturedContext context,
      StreamingCallback<K, V> callback) {
    return context == ExecutionContextProvider.CapturedContext.NO_OP ? callback : new CompletionGuard<>(callback);
  }

  static final class CompletionGuard<K, V> implements StreamingCallback<K, V> {
    private final StreamingCallback<K, V> callback;
    private int activeRecords;
    private boolean completed;
    private Optional<Exception> pendingCompletion;

    CompletionGuard(StreamingCallback<K, V> callback) {
      this.callback = callback;
    }

    @Override
    public void onRecordReceived(K key, V value) {
      runRecord(() -> callback.onRecordReceived(key, value));
    }

    void runRecord(Runnable recordDelivery) {
      synchronized (this) {
        if (completed) {
          return;
        }
        activeRecords++;
      }
      try {
        recordDelivery.run();
      } finally {
        Optional<Exception> completion;
        synchronized (this) {
          activeRecords--;
          completion = activeRecords == 0 ? pendingCompletion : null;
          if (completion != null) {
            pendingCompletion = null;
          }
        }
        if (completion != null) {
          callback.onCompletion(completion);
        }
      }
    }

    @Override
    public void onCompletion(Optional<Exception> exception) {
      synchronized (this) {
        if (completed) {
          return;
        }
        completed = true;
        if (activeRecords != 0) {
          pendingCompletion = exception;
          return;
        }
      }
      callback.onCompletion(exception);
    }
  }

  private static void run(
      ExecutionContextProvider.CapturedContext context,
      Runnable task,
      Consumer<Throwable> failureHandler) {
    ScopedTask scopedTask = new ScopedTask(task, failureHandler);
    try {
      context.run(scopedTask);
    } catch (Throwable failure) {
      if (scopedTask.failureReported) {
        throw failure;
      }
      scopedTask.reportFailure(failure);
    }
  }

  private static final class ScopedTask implements Runnable {
    private final Runnable task;
    private final Consumer<Throwable> failureHandler;
    private boolean failureReported;

    private ScopedTask(Runnable task, Consumer<Throwable> failureHandler) {
      this.task = task;
      this.failureHandler = failureHandler;
    }

    @Override
    public void run() {
      try {
        task.run();
      } catch (Throwable failure) {
        reportFailure(failure);
      }
    }

    private void reportFailure(Throwable failure) {
      failureReported = true;
      failureHandler.accept(failure);
    }
  }

  static CompletableFuture<Void> runAsync(
      ExecutionContextProvider.CapturedContext context,
      Runnable task,
      Executor executor) {
    if (context == ExecutionContextProvider.CapturedContext.NO_OP) {
      return CompletableFuture.runAsync(task, executor);
    }
    CompletableFuture<Void> completion = new CompletableFuture<>();
    executor.execute(wrap(context, () -> {
      task.run();
      completion.complete(null);
    }, failure -> completeExceptionally(completion, failure)));
    return completion;
  }

  static void completeExceptionally(CompletableFuture<?> completion, Throwable failure) {
    if (!completion.completeExceptionally(failure)) {
      LOGGER.error("Execution context task failed after completion", failure);
    }
  }
}
