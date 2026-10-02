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
  public void testRegisterReturnsResourceAndCloseIsOrderedIdempotentAndClosesLateRegistration() throws Exception {
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

    AutoCloseable late = mock(AutoCloseable.class);
    assertSame(scope.register(late), late);
    verify(late, times(1)).close();
  }

  @Test
  public void testRetireClosesOneResourceAndOnlyIfStillHeld() throws Exception {
    MetricScope scope = new MetricScope();
    AutoCloseable retired = scope.register(mock(AutoCloseable.class));
    AutoCloseable kept = scope.register(mock(AutoCloseable.class));

    scope.retire(retired);
    scope.retire(retired);
    verify(retired, times(1)).close();
    verify(kept, never()).close();

    scope.close();
    scope.retire(kept);
    verify(retired, times(1)).close();
    verify(kept, times(1)).close();
  }
}
