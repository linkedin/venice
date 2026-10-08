package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.function.BooleanSupplier;
import org.testng.annotations.Test;


public class VeniceControllerTest {
  @Test
  public void testReadinessRequiresCompletedStartupAndRunningServices() {
    VeniceController.ApiReadiness readiness = new VeniceController.ApiReadiness();
    BooleanSupplier servicesReady = mock(BooleanSupplier.class);
    when(servicesReady.getAsBoolean()).thenReturn(true);

    assertFalse(readiness.isReady(servicesReady));
    verifyNoInteractions(servicesReady);
    readiness.markReady();
    assertTrue(readiness.isReady(servicesReady));
    when(servicesReady.getAsBoolean()).thenReturn(false);
    assertFalse(readiness.isReady(servicesReady));
  }

  @Test
  public void testDrainingCannotBeUndoneByLateStartup() {
    VeniceController.ApiReadiness readiness = new VeniceController.ApiReadiness();
    readiness.drain();
    readiness.markReady();
    assertFalse(readiness.isReady(() -> true));

    VeniceController.ApiReadiness started = new VeniceController.ApiReadiness();
    started.markReady();
    assertTrue(started.isReady(() -> true));
    assertFalse(started.isReady(() -> {
      started.drain();
      return true;
    }));
    started.markReady();
    assertFalse(started.isReady(() -> true));
  }
}
