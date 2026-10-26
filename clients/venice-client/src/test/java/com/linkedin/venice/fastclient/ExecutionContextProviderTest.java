package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.store.streaming.StreamingCallback;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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
}
