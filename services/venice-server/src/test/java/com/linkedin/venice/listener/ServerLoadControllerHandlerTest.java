package com.linkedin.venice.listener;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.reliability.LoadController;
import com.linkedin.venice.stats.ServerLoadStats;
import com.linkedin.venice.utils.TestUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class ServerLoadControllerHandlerTest {
  private static final int SINGLE_GET_LATENCY_ACCEPTANCE_THRESHOLD = 10;
  private static final int MULTI_GET_LATENCY_ACCEPTANCE_THRESHOLD = 100;
  private static final int COMPUTE_LATENCY_ACCEPTANCE_THRESHOLD = 100;

  @Test
  public void testServerOverLoad() throws Exception {
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(serverConfig).isLoadControllerEnabled();
    doReturn(5).when(serverConfig).getLoadControllerWindowSizeInSec();
    doReturn(1.0d).when(serverConfig).getLoadControllerAcceptMultiplier();
    doReturn(0.9).when(serverConfig).getLoadControllerMaxRejectionRatio();
    doReturn(1).when(serverConfig).getLoadControllerRejectionRatioUpdateIntervalInSec();
    doReturn(SINGLE_GET_LATENCY_ACCEPTANCE_THRESHOLD).when(serverConfig)
        .getLoadControllerSingleGetLatencyAcceptThresholdMs();
    doReturn(MULTI_GET_LATENCY_ACCEPTANCE_THRESHOLD).when(serverConfig)
        .getLoadControllerMultiGetLatencyAcceptThresholdMs();
    doReturn(COMPUTE_LATENCY_ACCEPTANCE_THRESHOLD).when(serverConfig)
        .getLoadControllerComputeLatencyAcceptThresholdMs();

    ServerLoadStats loadStats = mock(ServerLoadStats.class);

    ServerLoadControllerHandler serverLoadControllerHandler = new ServerLoadControllerHandler(serverConfig, loadStats);

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    HttpRequest request = mock(HttpRequest.class);
    doReturn("/storage/abc").when(request).uri();

    // Record regular request
    serverLoadControllerHandler.recordLatency(RequestType.SINGLE_GET, 5, 200);
    serverLoadControllerHandler.recordLatency(RequestType.MULTI_GET, 50, 200);
    serverLoadControllerHandler.recordLatency(RequestType.COMPUTE, 50, 200);

    verify(loadStats, times(3)).recordAcceptedRequest();
    verify(loadStats, times(3)).recordTotalRequest();
    verify(loadStats, times(0)).recordRejectedRequest();

    LoadController loadController = serverLoadControllerHandler.getLoadController();
    assertEquals(loadController.getRejectionRatio(), 0.0d);

    // Record slow requests
    serverLoadControllerHandler.recordLatency(RequestType.SINGLE_GET, 20, 200);
    serverLoadControllerHandler.recordLatency(RequestType.MULTI_GET, 200, 200);
    serverLoadControllerHandler.recordLatency(RequestType.COMPUTE, 200, 200);
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> assertTrue(loadController.getRejectionRatio() > 0.0d));

    for (int i = 0; i < 100; ++i) {
      serverLoadControllerHandler.channelRead0(ctx, request);
    }
    // There must be some rejected requests
    verify(loadStats, atLeastOnce()).recordRejectedRequest();

    // Record overload requests
    serverLoadControllerHandler.recordLatency(RequestType.SINGLE_GET, 5, 529);
    serverLoadControllerHandler.recordLatency(RequestType.SINGLE_GET, 5, 529);

    verify(loadStats, times(3)).recordAcceptedRequest();
    verify(loadStats, times(8)).recordTotalRequest();

  }

}
