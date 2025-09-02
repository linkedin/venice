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
  private static final int RATE_DELTA = 30;

  @BeforeMethod
  public void setUp() {
    handlerMock = Mockito.mock(GlobalChannelTrafficShapingHandler.class);
    writeThrottler = new VeniceAdaptiveBlobTransferTrafficThrottler(3, BASE_RATE, RATE_DELTA, true);
    readThrottler = new VeniceAdaptiveBlobTransferTrafficThrottler(2, BASE_RATE, RATE_DELTA, false);

    writeThrottler.setGlobalChannelTrafficShapingHandler(handlerMock);
    readThrottler.setGlobalChannelTrafficShapingHandler(handlerMock);
  }

  @Test
  public void testLimiterSignalReducesRate() {
    // Multiple signal only result in one increase.
    writeThrottler.registerLimiterSignal(() -> true);
    writeThrottler.registerLimiterSignal(() -> true);

    writeThrottler.checkSignalAndAdjustThrottler();
    // Expect write limit reduced to 0.7 * BASE_RATE
    verify(handlerMock).setWriteLimit((long) (0.7 * BASE_RATE));

    writeThrottler.checkSignalAndAdjustThrottler();
    // Expect write limit reduced to 0.4 * BASE_RATE
    verify(handlerMock).setWriteLimit((long) (0.4 * BASE_RATE));

    writeThrottler.checkSignalAndAdjustThrottler();
    // Expect write limit reduced to 0.2 * BASE_RATE (which is the min rate)
    verify(handlerMock).setWriteLimit((long) (0.2 * BASE_RATE));

  }

  @Test
  public void testBoosterSignalIncreasesRate() {
    // Multiple signal only result in one increase.
    readThrottler.registerBoosterSignal(() -> true);
    readThrottler.registerBoosterSignal(() -> true);

    readThrottler.checkSignalAndAdjustThrottler();
    // Expect read limit increased to 1.3 * BASE_RATE
    verify(handlerMock).setReadLimit((long) (1.3 * BASE_RATE));

    readThrottler.checkSignalAndAdjustThrottler();
    // Expect read limit increased to 1.6 * BASE_RATE
    verify(handlerMock).setReadLimit((long) (1.6 * BASE_RATE));

    readThrottler.checkSignalAndAdjustThrottler();
    // Expect read limit increased to 1.9 * BASE_RATE
    verify(handlerMock).setReadLimit((long) (1.9 * BASE_RATE));

    readThrottler.checkSignalAndAdjustThrottler();
    // Expect read limit increased to 2.0 * BASE_RATE
    verify(handlerMock).setReadLimit((long) (2.0 * BASE_RATE));
  }

  @Test
  public void testIdleSignalIncreasesAfterThreshold() {
    // No signals → increments idle count
    for (int i = 0; i < 2; i++) {
      readThrottler.checkSignalAndAdjustThrottler();
    }

    // After idle threshold = 2, should increase by 30%
    verify(handlerMock).setReadLimit((long) (1.3 * BASE_RATE));
  }

  @Test
  public void testLimiterOverridesBooster() {
    writeThrottler.registerLimiterSignal(() -> true);
    writeThrottler.registerBoosterSignal(() -> true);

    writeThrottler.checkSignalAndAdjustThrottler();

    // Limiter should win
    verify(handlerMock).setWriteLimit((long) (0.7 * BASE_RATE));
    verify(handlerMock, never()).setReadLimit(anyLong());
  }

  @Test
  public void testNoHandlerDoesNotCrash() {
    VeniceAdaptiveBlobTransferTrafficThrottler throttler =
        new VeniceAdaptiveBlobTransferTrafficThrottler(2, BASE_RATE, RATE_DELTA, true);

    throttler.registerLimiterSignal(() -> true);

    // Should log but not throw exception
    throttler.checkSignalAndAdjustThrottler();
  }
}
