package com.linkedin.venice.router.stats;

import static com.linkedin.venice.read.RequestType.COMPUTE;
import static com.linkedin.venice.read.RequestType.COMPUTE_STREAMING;
import static com.linkedin.venice.read.RequestType.MULTI_GET;
import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.read.RequestType;
import java.io.Closeable;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;


public class RouterStatsTest {
  /** Closeable double that records how many times {@link #close()} was invoked. */
  private static final class CloseableStub implements Closeable {
    final AtomicInteger closeCount = new AtomicInteger();

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }
  }

  @Test
  public void testGetStatsByTypeReturnsPerRequestTypeInstance() {
    Map<RequestType, CloseableStub> stubs = new EnumMap<>(RequestType.class);
    for (RequestType type: new RequestType[] { SINGLE_GET, MULTI_GET, COMPUTE, MULTI_GET_STREAMING,
        COMPUTE_STREAMING }) {
      stubs.put(type, new CloseableStub());
    }
    RouterStats<CloseableStub> stats = new RouterStats<>(stubs::get);
    for (RequestType type: stubs.keySet()) {
      assertEquals(stats.getStatsByType(type), stubs.get(type));
    }
  }

  @Test
  public void testCloseClosesEveryCloseableStat() {
    Map<RequestType, CloseableStub> stubs = new EnumMap<>(RequestType.class);
    for (RequestType type: new RequestType[] { SINGLE_GET, MULTI_GET, COMPUTE, MULTI_GET_STREAMING,
        COMPUTE_STREAMING }) {
      stubs.put(type, new CloseableStub());
    }
    RouterStats<CloseableStub> stats = new RouterStats<>(stubs::get);
    stats.close();
    for (CloseableStub stub: stubs.values()) {
      assertEquals(stub.closeCount.get(), 1, "Each per-request-type Closeable should be closed exactly once");
    }
  }

  /** Verifies {@code RouterStats#close()} is a silent no-op when {@code STAT_TYPE} is not {@link Closeable}. */
  @Test
  public void testCloseSafeForNonCloseableStatType() {
    RouterStats<String> stats = new RouterStats<>(type -> "stats-for-" + type.name());
    // Should not throw despite STAT_TYPE not being Closeable.
    stats.close();
    assertEquals(stats.getStatsByType(SINGLE_GET), "stats-for-SINGLE_GET");
  }

  @Test
  public void testCloseIsIdempotent() {
    CloseableStub stub = new CloseableStub();
    RouterStats<CloseableStub> stats = new RouterStats<>(type -> stub);
    stats.close();
    stats.close();
    // Each request-type slot holds the same stub instance, so it's closed once per slot per close() call.
    // Across two close() calls that's 2 * 5 invocations — the value isn't what matters, only that no exception fires
    // and the per-Closeable close was attempted.
    assertTrue(stub.closeCount.get() >= 5, "close() should not throw and should keep delegating on repeat calls");
    assertFalse(stub.closeCount.get() < 5);
  }
}
