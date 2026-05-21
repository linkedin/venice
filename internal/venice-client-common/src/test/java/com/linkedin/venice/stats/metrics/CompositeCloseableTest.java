package com.linkedin.venice.stats.metrics;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link CompositeCloseable}. The class is a load-bearing piece of the OTel
 * runtime cleanup framework; ~100 production sites depend on the contract verified here.
 */
public class CompositeCloseableTest {
  /** Records its index when close() is invoked, so tests can verify ordering. */
  private static final class IndexRecordingCloseable implements Closeable {
    private final List<Integer> closeOrder;
    private final int myIndex;
    private boolean closed = false;

    IndexRecordingCloseable(List<Integer> closeOrder, int myIndex) {
      this.closeOrder = closeOrder;
      this.myIndex = myIndex;
    }

    @Override
    public void close() {
      closed = true;
      closeOrder.add(myIndex);
    }

    boolean isClosed() {
      return closed;
    }
  }

  private static final class ThrowingCloseable implements Closeable {
    private final RuntimeException toThrow;
    boolean closeCalled = false;

    ThrowingCloseable(RuntimeException toThrow) {
      this.toThrow = toThrow;
    }

    @Override
    public void close() {
      closeCalled = true;
      throw toThrow;
    }
  }

  private static final class CountingCloseable implements Closeable {
    int closeCount = 0;

    @Override
    public void close() {
      closeCount++;
    }
  }

  @Test
  public void testRegisterReturnsResourceForFluentAssignment() {
    CompositeCloseable resources = new CompositeCloseable();
    CountingCloseable c = new CountingCloseable();
    Closeable returned = resources.register(c);
    assertSame(returned, c, "register() must return the same instance for fluent assignment");
  }

  @Test
  public void testRegisterNullIsSilentlyIgnored() {
    CompositeCloseable resources = new CompositeCloseable();
    // Must not throw.
    Closeable returned = resources.register(null);
    assertNull(returned, "register(null) must return null");
    resources.close(); // Must not throw despite no resources.
  }

  @Test
  public void testCloseInvokesAllRegisteredResourcesInReverseOrder() {
    CompositeCloseable resources = new CompositeCloseable();
    List<Integer> closeOrder = new ArrayList<>();
    IndexRecordingCloseable a = new IndexRecordingCloseable(closeOrder, 0);
    IndexRecordingCloseable b = new IndexRecordingCloseable(closeOrder, 1);
    IndexRecordingCloseable c = new IndexRecordingCloseable(closeOrder, 2);
    resources.register(a);
    resources.register(b);
    resources.register(c);

    resources.close();

    assertTrue(a.isClosed());
    assertTrue(b.isClosed());
    assertTrue(c.isClosed());
    assertEquals(closeOrder, Arrays.asList(2, 1, 0), "close() must run in reverse registration order");
  }

  @Test
  public void testCloseIsIdempotent() {
    CompositeCloseable resources = new CompositeCloseable();
    CountingCloseable c = new CountingCloseable();
    resources.register(c);

    resources.close();
    resources.close();
    resources.close();

    assertEquals(c.closeCount, 1, "Resource must be closed exactly once across multiple close() calls");
  }

  @Test
  public void testThrowingCloseableDoesNotAbortIteration() {
    // Verifies the documented contract: "a misbehaving wrapper cannot block shutdown."
    // closeQuietly catches Exception, so RuntimeException from one resource must NOT
    // skip the close() of the others.
    CompositeCloseable resources = new CompositeCloseable();
    CountingCloseable before = new CountingCloseable();
    ThrowingCloseable throwing = new ThrowingCloseable(new IllegalStateException("simulated bad close"));
    CountingCloseable after = new CountingCloseable();
    resources.register(before);
    resources.register(throwing);
    resources.register(after);

    resources.close();

    assertTrue(throwing.closeCalled, "Throwing resource's close() must have been invoked");
    // Reverse order: after closes first, then throwing (throws), then before must still close.
    assertEquals(after.closeCount, 1, "Sibling registered after the throwing resource must still close");
    assertEquals(before.closeCount, 1, "Sibling registered before the throwing resource must still close");
  }

  @Test
  public void testThrowingCloseableHandlesRuntimeExceptionTypes() {
    // Verifies catch is broad enough for the various runtime exceptions the OTel SDK can
    // produce: NPE from a half-init instrument, ClassCastException from a wrong cast, etc.
    CompositeCloseable resources = new CompositeCloseable();
    resources.register(new ThrowingCloseable(new NullPointerException("simulated NPE")));
    resources.register(new ThrowingCloseable(new ClassCastException("simulated CCE")));
    resources.register(new ThrowingCloseable(new IllegalStateException("simulated ISE")));

    // Must not throw; all swallowed by closeQuietly.
    resources.close();
  }

  @Test
  public void testCloseableThatThrowsIOExceptionIsAlsoSwallowed() {
    CompositeCloseable resources = new CompositeCloseable();
    resources.register(() -> {
      throw new IOException("simulated IO failure");
    });
    // Must not propagate IOException either.
    resources.close();
  }

  @Test
  public void testRegisterAfterCloseImmediatelyClosesResource() {
    // After close(), the registry is in a closed state. Subsequent register() must close
    // the resource immediately rather than tracking it (preventing leaks from racy shutdown
    // ordering, e.g., a per-store map populated mid-shutdown).
    CompositeCloseable resources = new CompositeCloseable();
    resources.close();

    CountingCloseable late = new CountingCloseable();
    Closeable returned = resources.register(late);

    assertSame(returned, late, "register() after close still returns the resource for assignment");
    assertEquals(late.closeCount, 1, "Resource registered after close() must be closed immediately");
  }

  @Test
  public void testRegisterAfterCloseDoesNotResurrect() {
    // Calling close() again after a post-close register() must not re-close the resource.
    CompositeCloseable resources = new CompositeCloseable();
    resources.close();
    CountingCloseable late = new CountingCloseable();
    resources.register(late);

    resources.close();

    assertEquals(late.closeCount, 1, "Resource must not be closed twice on a closed registry");
  }

  @Test
  public void testNoneSentinelDoesNotTrack() {
    // CompositeCloseable.NONE must be a no-op: register() returns the resource without tracking,
    // close() does nothing. Tests rely on this so passing NONE doesn't accumulate state across
    // unrelated tests.
    CountingCloseable c = new CountingCloseable();
    Closeable returned = CompositeCloseable.NONE.register(c);
    assertSame(returned, c, "NONE.register() must return the resource");

    CompositeCloseable.NONE.close();

    assertEquals(c.closeCount, 0, "NONE must not track or close the registered resource");
  }

  @Test
  public void testNoneIsThreadSafeAndIdempotent() {
    // Sanity: NONE has no internal state to corrupt, so this is just a smoke test.
    for (int i = 0; i < 100; i++) {
      CompositeCloseable.NONE.register(new CountingCloseable());
      CompositeCloseable.NONE.close();
    }
  }

  @Test
  public void testConcurrentRegisterIsThreadSafe() throws InterruptedException {
    // Even though the production usage pattern is "register at construction time" (single-threaded),
    // the contract claims thread safety. Verify by stressing register() from multiple threads.
    CompositeCloseable resources = new CompositeCloseable();
    int threadCount = 8;
    int perThread = 500;
    AtomicInteger closeCallCount = new AtomicInteger();
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);

    try {
      for (int t = 0; t < threadCount; t++) {
        pool.submit(() -> {
          start.await();
          for (int i = 0; i < perThread; i++) {
            resources.register(closeCallCount::incrementAndGet);
          }
          return null;
        });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

      resources.close();

      assertEquals(
          closeCallCount.get(),
          threadCount * perThread,
          "Every concurrently-registered resource must be closed exactly once");
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void testConcurrentRegisterAndCloseDoesNotLeak() throws InterruptedException {
    // Race: one thread closes while another registers. The closed-flag guard ensures any
    // resource that races past close() is closed immediately rather than leaking.
    int rounds = 100;
    for (int round = 0; round < rounds; round++) {
      CompositeCloseable resources = new CompositeCloseable();
      AtomicInteger closeCallCount = new AtomicInteger();
      CountDownLatch ready = new CountDownLatch(2);
      CountDownLatch fire = new CountDownLatch(1);
      ExecutorService pool = Executors.newFixedThreadPool(2);

      pool.submit(() -> {
        ready.countDown();
        fire.await();
        for (int i = 0; i < 100; i++) {
          resources.register(closeCallCount::incrementAndGet);
        }
        return null;
      });
      pool.submit(() -> {
        ready.countDown();
        fire.await();
        resources.close();
        return null;
      });

      ready.await();
      fire.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

      // Guarantee: any resource registered before close completed was closed; any registered
      // after was closed immediately by the post-close guard. Net: closeCount == 100.
      assertEquals(closeCallCount.get(), 100, "All registered resources must be closed even under register/close race");
    }
  }
}
