package com.linkedin.davinci.blobtransfer;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceAdaptiveBlobTransferTrafficThrottlerTest {
  private VeniceAdaptiveBlobTransferTrafficThrottler writeThrottler;
  private VeniceAdaptiveBlobTransferTrafficThrottler readThrottler;
  private GlobalChannelTrafficShapingHandler handlerMock;

  private static final long BASE_RATE = 1000L;

  @BeforeMethod
  public void setUp() {
    handlerMock = Mockito.mock(GlobalChannelTrafficShapingHandler.class);
    writeThrottler = new VeniceAdaptiveBlobTransferTrafficThrottler(3, BASE_RATE, true);
    readThrottler = new VeniceAdaptiveBlobTransferTrafficThrottler(2, BASE_RATE, false);

    writeThrottler.setGlobalChannelTrafficShapingHandler(handlerMock);
    readThrottler.setGlobalChannelTrafficShapingHandler(handlerMock);
  }

  @Test
  public void testLimiterSignalReducesRate() {
    writeThrottler.registerLimiterSignal(() -> true);

    writeThrottler.checkSignalAndAdjustThrottler();

    // Expect write limit reduced to 0.8 * BASE_RATE
    verify(handlerMock).setWriteLimit((long) (0.8 * BASE_RATE));
  }

  @Test
  public void testBoosterSignalIncreasesRate() {
    readThrottler.registerBoosterSignal(() -> true);

    readThrottler.checkSignalAndAdjustThrottler();

    // Expect read limit increased to 1.2 * BASE_RATE
    verify(handlerMock).setReadLimit((long) (1.2 * BASE_RATE));
  }

  @Test
  public void testIdleSignalIncreasesAfterThreshold() {
    // No signals → increments idle count
    for (int i = 0; i < 2; i++) {
      readThrottler.checkSignalAndAdjustThrottler();
    }

    // After idle threshold = 2, should increase by 20%
    verify(handlerMock).setReadLimit((long) (1.2 * BASE_RATE));
  }

  @Test
  public void testLimiterOverridesBooster() {
    writeThrottler.registerLimiterSignal(() -> true);
    writeThrottler.registerBoosterSignal(() -> true);

    writeThrottler.checkSignalAndAdjustThrottler();

    // Limiter should win
    verify(handlerMock).setWriteLimit((long) (0.8 * BASE_RATE));
    verify(handlerMock, never()).setReadLimit(anyLong());
  }

  @Test
  public void testNoHandlerDoesNotCrash() {
    VeniceAdaptiveBlobTransferTrafficThrottler throttler =
        new VeniceAdaptiveBlobTransferTrafficThrottler(2, BASE_RATE, true);

    throttler.registerLimiterSignal(() -> true);

    // Should log but not throw exception
    throttler.checkSignalAndAdjustThrottler();
  }
}
