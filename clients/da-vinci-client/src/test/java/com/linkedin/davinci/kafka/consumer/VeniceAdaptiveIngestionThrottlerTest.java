package com.linkedin.davinci.kafka.consumer;

import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceAdaptiveIngestionThrottlerTest {
  @Test
  public void testAdaptiveIngestionThrottler() {
    List<Double> factors = Arrays.asList(0.4D, 0.6D, 0.8D, 1.0D, 1.2D, 1.4D);
    VeniceAdaptiveIngestionThrottler adaptiveIngestionThrottler =
        new VeniceAdaptiveIngestionThrottler(10, 100, factors, 10, "test");
    adaptiveIngestionThrottler.registerLimiterSignal(() -> true);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 1);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 0);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 0);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 0);
    adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(10, 100, factors, 10, "test");
    adaptiveIngestionThrottler.registerBoosterSignal(() -> true);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 4);

    adaptiveIngestionThrottler = new VeniceAdaptiveIngestionThrottler(3, 100, factors, 10, "test");

    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 3);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 3);
    adaptiveIngestionThrottler.checkSignalAndAdjustThrottler();
    Assert.assertEquals(adaptiveIngestionThrottler.getCurrentThrottlerIndex(), 4);
  }
}
