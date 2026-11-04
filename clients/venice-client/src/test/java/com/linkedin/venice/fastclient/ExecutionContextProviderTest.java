package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ExecutionContextProviderTest {
  @Test
  public void testNoOpDoesNotDecorateTasksOrCallbacks() {
    ExecutionContextProvider.CapturedContext context = ExecutionContextProvider.NO_OP.capture();
    assertSame(context, ExecutionContextProvider.NO_OP.capture());
    Runnable task = () -> {};
    BiConsumer<String, Throwable> completion = (value, failure) -> {};
    StreamingCallback<String, String> callback = mock(StreamingCallback.class);
    assertSame(context.wrap(task), task);
    assertSame(context.wrap(completion), completion);
    assertSame(context.wrap(callback), callback);
    assertSame(ExecutionContextUtils.guardCompletion(context, callback), callback);
    Consumer<Throwable> failureHandler = failure -> {
      throw new AssertionError("The no-op context must not need a failure guard", failure);
    };
    assertSame(ExecutionContextUtils.wrap(context, task, failureHandler), task);
    assertSame(ExecutionContextUtils.wrap(context, completion, failureHandler), completion);
    AtomicInteger calls = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("task");
    assertSame(expectThrows(IllegalStateException.class, () -> context.wrap(() -> {
      calls.incrementAndGet();
      throw failure;
    }).run()), failure);
    assertEquals(calls.get(), 1);
  }

  @Test(timeOut = 15_000)
  public void testConcurrentCompletionWaitsForThrowingRecordAndRejectsLateRecords() throws Exception {
    ThreadLocal<String> context = new ThreadLocal<>();
    ExecutionContextProvider.CapturedContext captured = task -> {
      String previous = context.get();
      context.set("request");
      try {
        task.run();
      } finally {
        context.set(previous);
      }
    };
    CompletableFuture<Void> entered = new CompletableFuture<>();
    CompletableFuture<Void> release = new CompletableFuture<>();
    CompletableFuture<Optional<Exception>> completion = new CompletableFuture<>();
    AtomicInteger records = new AtomicInteger();
    AtomicInteger completions = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("completion");
    captured.<String, Throwable>wrap((value, error) -> {
      assertEquals(value, "value");
      assertSame(error, failure);
      assertEquals(context.get(), "request");
    }).accept("value", failure);
    assertNull(context.get());
    AtomicReference<StreamingCallback<String, String>> guarded = new AtomicReference<>();
    guarded.set(ExecutionContextUtils.guardCompletion(captured, new StreamingCallback<String, String>() {
      @Override
      public void onRecordReceived(String key, String value) {
        assertEquals(context.get(), "request");
        assertEquals(key, "key", "Late records must never enter application code");
        assertEquals(value, "value");
        records.incrementAndGet();
        entered.complete(null);
        release.join();
        guarded.get().onCompletion(Optional.empty());
        assertFalse(completion.isDone(), "Completion must wait until the active callback returns");
        throw failure;
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        assertEquals(context.get(), "request");
        completions.incrementAndGet();
        completion.complete(exception);
        throw failure;
      }
    }));
    StreamingCallback<String, String> scoped = captured.wrap(guarded.get());
    CompletableFuture<Void> active = CompletableFuture.runAsync(() -> {
      try {
        scoped.onRecordReceived("key", "value");
      } finally {
        assertNull(context.get());
      }
    });
    try {
      entered.get(5, TimeUnit.SECONDS);
      CompletableFuture.runAsync(() -> scoped.onCompletion(Optional.of(failure))).get(5, TimeUnit.SECONDS);
      scoped.onRecordReceived("late", "ignored");
      assertFalse(completion.isDone());
      assertNull(context.get());
    } finally {
      release.complete(null);
    }
    assertSame(expectThrows(ExecutionException.class, () -> active.get(5, TimeUnit.SECONDS)).getCause(), failure);
    assertSame(completion.get(5, TimeUnit.SECONDS).get(), failure);
    guarded.get().onCompletion(Optional.empty());
    assertEquals(records.get(), 1);
    assertEquals(completions.get(), 1, "A throwing terminal callback must not be replayed");
  }

  @DataProvider
  public Object[][] scopedFailureHandlers() {
    return new Object[][] { { false, false }, { true, false }, { false, true }, { true, true } };
  }

  @Test(dataProvider = "scopedFailureHandlers")
  public void testTaskFailureIsReportedWithinScopeWithoutReplayingHandler(
      boolean handlerThrows,
      boolean cleanupThrows) {
    AtomicInteger scopeDepth = new AtomicInteger();
    AtomicInteger notifications = new AtomicInteger();
    IllegalStateException taskFailure = new IllegalStateException("task");
    IllegalArgumentException handlerFailure = new IllegalArgumentException("handler");
    IllegalStateException cleanupFailure = new IllegalStateException("cleanup");
    ExecutionContextProvider.CapturedContext context = task -> {
      scopeDepth.incrementAndGet();
      try {
        task.run();
      } finally {
        scopeDepth.decrementAndGet();
        if (cleanupThrows) {
          throw cleanupFailure;
        }
      }
    };
    Runnable guarded = ExecutionContextUtils.wrap(context, () -> {
      throw taskFailure;
    }, failure -> {
      notifications.incrementAndGet();
      assertEquals(scopeDepth.get(), 1, "Task failures must be reported before restoring worker context");
      assertSame(failure, taskFailure);
      if (handlerThrows) {
        throw handlerFailure;
      }
    });
    if (handlerThrows || cleanupThrows) {
      assertSame(expectThrows(RuntimeException.class, guarded::run), cleanupThrows ? cleanupFailure : handlerFailure);
    } else {
      guarded.run();
    }
    assertEquals(scopeDepth.get(), 0);
    assertEquals(notifications.get(), 1);
  }
}
