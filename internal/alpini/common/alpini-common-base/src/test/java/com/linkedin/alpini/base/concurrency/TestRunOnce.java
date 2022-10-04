package com.linkedin.alpini.base.concurrency;

import org.mockito.Mockito;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class TestRunOnce {
  @Test(groups = "unit")
  public void testIt() {
    Runnable mock = Mockito.mock(Runnable.class);
    Runnable runOnce = RunOnce.make(mock);

    runOnce.run();
    runOnce.run();

    Mockito.verify(mock).run();
    ;
    Mockito.verifyNoMoreInteractions(mock);
  }
}
