package com.linkedin.venice.controller.multitaskscheduler;

import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class MultiTaskSchedulerServiceTest {
  @Mock
  private StoreMigrationManager mockStoreMigrationManager;

  private MultiTaskSchedulerService service;

  @BeforeTest
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    service = new MultiTaskSchedulerService(4, 3, 1, new ArrayList<>());
    // Inject the mock StoreMigrationManager into the service
    // Use AccessController.doPrivileged block to set the field accessible
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field field = MultiTaskSchedulerService.class.getDeclaredField("storeMigrationManager");
        field.setAccessible(true);
        field.set(service, mockStoreMigrationManager);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  @Test(priority = 1)
  public void testStartInner() throws Exception {
    boolean result = service.startInner();
    assertTrue(result);
  }

  @Test(priority = 2)
  public void testStopInner() throws Exception {
    service.stopInner();
    verify(mockStoreMigrationManager).shutdown();
  }

  @Test(priority = 3)
  public void testStart() {
    service.start();
    assertTrue(service.isRunning());
  }

  @Test(priority = 4)
  public void testStop() throws Exception {
    service.stop();
    assertFalse(service.isRunning());
  }
}
