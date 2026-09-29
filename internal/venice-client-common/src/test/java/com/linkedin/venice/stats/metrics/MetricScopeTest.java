package com.linkedin.venice.stats.metrics;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import org.mockito.InOrder;
import org.testng.annotations.Test;


/** Tests {@link MetricScope} close ordering, idempotency and late registration. */
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
}
