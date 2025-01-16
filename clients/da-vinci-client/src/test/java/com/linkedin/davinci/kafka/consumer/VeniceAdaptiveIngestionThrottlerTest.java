package com.linkedin.davinci.kafka.consumer;

import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceAdaptiveIngestionThrottlerTest {
  @Test
  public void testAdaptiveIngestionThrottler() {
    VeniceAdaptiveIngestionThrottler adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(100, 10, "test");
    adaptiveIngestionThrottler.registerLimiterSignal(() -> true);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 2);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 1);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 0);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 0);
    adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(100, 10, "test");
    adaptiveIngestionThrottler.registerBoosterSignal(() -> true);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 4);
  }
}
