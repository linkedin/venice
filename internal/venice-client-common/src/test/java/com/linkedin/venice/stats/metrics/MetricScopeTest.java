package com.linkedin.venice.stats.metrics;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.mockito.InOrder;
import org.testng.annotations.Test;


/** Tests {@link MetricScope} close ordering, idempotency, late registration and early retirement. */
public class MetricScopeTest {
  @Test
  public void testRegisterReturnsResourceAndCloseIsOrderedAndIdempotent() throws Exception {
    MetricScope scope = new MetricScope();
    AutoCloseable first = mock(AutoCloseable.class);
    AutoCloseable second = mock(AutoCloseable.class);

    assertSame(scope.register(first), first);
    assertSame(scope.register(second), second);
    assertFalse(scope.isClosed());

    scope.close();
    scope.close();

    assertTrue(scope.isClosed());
    InOrder inOrder = inOrder(first, second);
    inOrder.verify(first).close();
    inOrder.verify(second).close();
    verify(first, times(1)).close();
    verify(second, times(1)).close();
  }

  @Test
  public void testRegisterAfterCloseClosesImmediately() throws Exception {
    MetricScope scope = new MetricScope();
    scope.close();

    AutoCloseable resource = mock(AutoCloseable.class);
    assertSame(scope.register(resource), resource);

    verify(resource, times(1)).close();
  }

  @Test
  public void testRetireClosesOneResourceAndStopsHoldingIt() throws Exception {
    MetricScope scope = new MetricScope();
    AutoCloseable retired = scope.register(mock(AutoCloseable.class));
    AutoCloseable kept = scope.register(mock(AutoCloseable.class));

    scope.retire(retired);
    verify(retired, times(1)).close();
    verify(kept, never()).close();

    // The scope no longer holds the retired resource, so closing the scope doesn't close it again.
    scope.close();
    verify(retired, times(1)).close();
    verify(kept, times(1)).close();
  }

  @Test
  public void testRetireClosesOnlyAResourceTheScopeStillHolds() throws Exception {
    MetricScope scope = new MetricScope();
    AutoCloseable retiredTwice = scope.register(mock(AutoCloseable.class));
    scope.retire(retiredTwice);
    scope.retire(retiredTwice);
    verify(retiredTwice, times(1)).close();

    // A resource the scope already closed isn't closed again by a later retire.
    AutoCloseable closedWithScope = scope.register(mock(AutoCloseable.class));
    scope.close();
    scope.retire(closedWithScope);
    verify(closedWithScope, times(1)).close();
  }
}
