package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
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
    StreamingCallback<String, String> callback = new StreamingCallback<String, String>() {
      @Override
      public void onRecordReceived(String key, String value) {
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
      }
    };
    assertSame(context.wrap(task), task);
    assertSame(context.wrap(completion), completion);
    assertSame(context.wrap(callback), callback);
    Consumer<Throwable> failureHandler = failure -> {
      throw new AssertionError("The no-op context must not need a failure guard", failure);
    };
    assertSame(ExecutionContextUtils.wrap(context, task, failureHandler), task);
    assertSame(ExecutionContextUtils.wrap(context, completion, failureHandler), completion);
  }

  @Test
  public void testDirectExecutionAndExceptionPropagation() {
    ExecutionContextProvider.CapturedContext context = ExecutionContextProvider.NO_OP.capture();
    AtomicInteger calls = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("task");
    assertSame(expectThrows(IllegalStateException.class, () -> context.wrap(() -> {
      calls.incrementAndGet();
      throw failure;
    }).run()), failure);
    assertEquals(calls.get(), 1);
  }

  @Test
  public void testWrappedCallbacksUseCapturedScopeAndRestoreAfterException() {
    AtomicInteger scopeDepth = new AtomicInteger();
    ExecutionContextProvider.CapturedContext context = task -> {
      scopeDepth.incrementAndGet();
      try {
        task.run();
      } finally {
        scopeDepth.decrementAndGet();
      }
    };
    IllegalArgumentException failure = new IllegalArgumentException("callback");
    context.<String, Throwable>wrap((value, exception) -> {
      assertEquals(value, "value");
      assertSame(exception, failure);
      assertEquals(scopeDepth.get(), 1);
    }).accept("value", failure);
    AtomicInteger records = new AtomicInteger();
    StreamingCallback<String, String> callback = context.wrap(new StreamingCallback<String, String>() {
      @Override
      public void onRecordReceived(String key, String value) {
        assertEquals(scopeDepth.get(), 1);
        assertEquals(key, "key");
        assertEquals(value, "value");
        records.incrementAndGet();
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        assertEquals(scopeDepth.get(), 1);
        assertSame(exception.get(), failure);
        throw failure;
      }
    });
    callback.onRecordReceived("key", "value");
    assertEquals(scopeDepth.get(), 0);
    assertSame(
        expectThrows(IllegalArgumentException.class, () -> callback.onCompletion(Optional.of(failure))),
        failure);
    assertEquals(scopeDepth.get(), 0);
    assertEquals(records.get(), 1);
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
    AtomicInteger tasks = new AtomicInteger();
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
      tasks.incrementAndGet();
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
    assertEquals(tasks.get(), 1);
    assertEquals(notifications.get(), 1);
  }
}
