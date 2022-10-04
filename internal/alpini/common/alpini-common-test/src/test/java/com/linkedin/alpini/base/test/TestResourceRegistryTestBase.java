package com.linkedin.alpini.base.test;

import com.linkedin.alpini.base.registry.Shutdownable;
import com.linkedin.alpini.base.registry.SyncShutdownable;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class TestResourceRegistryTestBase extends ResourceRegistryTestBase {
  private static Shutdownable _shutdownable = Mockito.mock(Shutdownable.class);
  private static SyncShutdownable _syncShutdownable = Mockito.mock(SyncShutdownable.class);

  @Test
  public void basicTest() {
    Assert.assertSame(register(_shutdownable), _shutdownable);
    Assert.assertSame(register(_syncShutdownable), _syncShutdownable);
  }

  @AfterClass(dependsOnMethods = "shutdownResourceRegistry")
  public static void afterClass() throws InterruptedException {
    Mockito.verify(_shutdownable).shutdown();
    Mockito.verify(_shutdownable).waitForShutdown();
    Mockito.verify(_syncShutdownable).shutdownSynchronously();
    Mockito.verifyNoMoreInteractions(_shutdownable, _syncShutdownable);
  }
}
