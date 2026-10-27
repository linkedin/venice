package com.linkedin.venice.fastclient;

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

  private static void run(
      ExecutionContextProvider.CapturedContext context,
      Runnable task,
      Consumer<Throwable> failureHandler) {
    try {
      context.run(task);
    } catch (Throwable failure) {
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
