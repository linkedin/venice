package com.linkedin.davinci.blobtransfer;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.stats.AdaptiveThrottlingServiceStats;
import com.linkedin.venice.stats.dimensions.VeniceAdaptiveThrottlerType;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceAdaptiveBlobTransferTrafficThrottlerTest {
  private VeniceAdaptiveBlobTransferTrafficThrottler writeThrottler;
  private VeniceAdaptiveBlobTransferTrafficThrottler readThrottler;
  private GlobalChannelTrafficShapingHandler handlerMock;
  private AdaptiveThrottlingServiceStats statsMock;

  private static final long BASE_RATE = 1000L;
  private static final int RATE_DELTA = 30;

  @BeforeMethod
  public void setUp() {
    handlerMock = Mockito.mock(GlobalChannelTrafficShapingHandler.class);
    statsMock = Mockito.mock(AdaptiveThrottlingServiceStats.class);
    writeThrottler = new VeniceAdaptiveBlobTransferTrafficThrottler(3, BASE_RATE, RATE_DELTA, true, statsMock);
    readThrottler = new VeniceAdaptiveBlobTransferTrafficThrottler(2, BASE_RATE, RATE_DELTA, false, statsMock);

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
    // Fresh stats so this assertion is isolated from the handler-wiring seed done in setUp().
    AdaptiveThrottlingServiceStats freshStats = Mockito.mock(AdaptiveThrottlingServiceStats.class);
    VeniceAdaptiveBlobTransferTrafficThrottler throttler =
        new VeniceAdaptiveBlobTransferTrafficThrottler(2, BASE_RATE, RATE_DELTA, true, freshStats);

    throttler.registerLimiterSignal(() -> true);

    // No traffic shaping handler is wired, so the throttler is inactive: it neither throws nor records.
    throttler.checkSignalAndAdjustThrottler();
    verify(freshStats, never()).recordRateForAdaptiveThrottler(Mockito.any(), anyLong());
  }

  @Test
  public void testSeedsCurrentLimitWhenHandlerWired() {
    AdaptiveThrottlingServiceStats freshStats = Mockito.mock(AdaptiveThrottlingServiceStats.class);
    VeniceAdaptiveBlobTransferTrafficThrottler throttler =
        new VeniceAdaptiveBlobTransferTrafficThrottler(2, BASE_RATE, RATE_DELTA, true, freshStats);

    // Wiring the handler seeds the gauge with the configured base limit immediately (no 0 until the first tick).
    throttler.setGlobalChannelTrafficShapingHandler(Mockito.mock(GlobalChannelTrafficShapingHandler.class));
    verify(freshStats)
        .recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType.BLOB_TRANSFER_WRITE_BANDWIDTH, BASE_RATE);
  }

  @Test
  public void testRecordsWriteRateUnderWriteType() {
    writeThrottler.registerLimiterSignal(() -> true);

    writeThrottler.checkSignalAndAdjustThrottler();

    // After one limiter signal the write factor drops to 70%, i.e. 0.7 * BASE_RATE bytes/sec.
    verify(statsMock).recordRateForAdaptiveThrottler(
        VeniceAdaptiveThrottlerType.BLOB_TRANSFER_WRITE_BANDWIDTH,
        (long) (0.7 * BASE_RATE));
  }

  @Test
  public void testRecordsReadRateUnderReadType() {
    readThrottler.registerBoosterSignal(() -> true);

    readThrottler.checkSignalAndAdjustThrottler();

    // A booster signal raises the read factor to 130%, i.e. 1.3 * BASE_RATE bytes/sec, recorded under the READ type.
    verify(statsMock).recordRateForAdaptiveThrottler(
        VeniceAdaptiveThrottlerType.BLOB_TRANSFER_READ_BANDWIDTH,
        (long) (1.3 * BASE_RATE));
  }
}
