package com.linkedin.davinci.ingestion.main;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.IsolatedIngestionProcessStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.IngestionMetricsReport;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionReportType;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MainIngestionReportHandlerTest {
  @Test
  public void testUpdateMetrics() {
    MainIngestionMonitorService ingestionMonitorService = mock(MainIngestionMonitorService.class);
    MetricsRepository metricsRepository = new MetricsRepository();
    IsolatedIngestionProcessStats ingestionProcessStats = new IsolatedIngestionProcessStats(metricsRepository);
    when(ingestionMonitorService.getIsolatedIngestionProcessStats()).thenReturn(ingestionProcessStats);
    MainIngestionReportHandler ingestionReportHandler = new MainIngestionReportHandler(ingestionMonitorService);
    IngestionMetricsReport ingestionMetricsReport = new IngestionMetricsReport();
    ingestionMetricsReport.aggregatedMetrics = new HashMap<>();
    ingestionMetricsReport.aggregatedMetrics.put("foo.AVG", 2.0);
    ingestionMetricsReport.aggregatedMetrics.put("foo.MAX", 3.0);
    ingestionMetricsReport.aggregatedMetrics.put("foo.MIN", 1.0);
    ingestionMetricsReport.aggregatedMetrics.put(".bar", 4.0);
    ingestionMetricsReport.aggregatedMetrics.put("a.b.c.d", 5.0);
    ingestionReportHandler.handleMetricsReport(ingestionMetricsReport);
    LogManager.getLogger().info(metricsRepository.metrics().keySet());

    Assert.assertEquals(metricsRepository.metrics().size(), 5);
    ingestionMetricsReport.aggregatedMetrics = new HashMap<>();
    ingestionMetricsReport.aggregatedMetrics.put(".bar", 6.0);
    ingestionReportHandler.handleMetricsReport(ingestionMetricsReport);
    Assert.assertEquals(metricsRepository.metrics().size(), 5);
    LogManager.getLogger().info(metricsRepository.metrics().keySet());
    Assert.assertEquals(metricsRepository.getMetric(".ingestion_isolation--bar.Gauge").value(), 6.0);
    Assert.assertEquals(metricsRepository.getMetric(".ingestion_isolation--foo.AVG").value(), 2.0);
  }

  @Test
  public void testHandleIngestionReport() {
    MainIngestionMonitorService ingestionMonitorService = mock(MainIngestionMonitorService.class);
    MainIngestionReportHandler ingestionReportHandler = new MainIngestionReportHandler(ingestionMonitorService);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

    VeniceNotifier ingestionNotifier = mock(VeniceNotifier.class);
    VeniceNotifier pushStatusNotifier = mock(VeniceNotifier.class);
    when(ingestionMonitorService.getIngestionNotifier()).thenReturn(Collections.singletonList(ingestionNotifier));
    when(ingestionMonitorService.getPushStatusNotifierList()).thenReturn(Collections.singletonList(pushStatusNotifier));

    IngestionTaskReport ingestionTaskReport = new IngestionTaskReport();
    ingestionTaskReport.reportType = IngestionReportType.COMPLETED.getValue();
    ingestionTaskReport.isPositive = true;
    ingestionTaskReport.topicName = "topic";
    ingestionTaskReport.partitionId = 0;
    ingestionTaskReport.message = "";
    ingestionTaskReport.offsetRecordArray = Collections.emptyList();

    FullHttpRequest msg = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.POST,
        "http://127.0.0.1:27015/REPORT",
        Unpooled.wrappedBuffer(
            AvroProtocolDefinition.INGESTION_TASK_REPORT.getSerializer().serialize(null, ingestionTaskReport)));
    ingestionReportHandler.channelRead0(ctx, msg);
    verify(ingestionNotifier, times(1)).completed(anyString(), anyInt(), anyLong(), anyString(), any());
    verify(pushStatusNotifier, times(1)).completed(anyString(), anyInt(), anyLong(), anyString(), any());
  }

  @Test
  public void testHandleException() {
    MainIngestionMonitorService ingestionMonitorService = mock(MainIngestionMonitorService.class);
    MainIngestionReportHandler ingestionReportHandler = new MainIngestionReportHandler(ingestionMonitorService);
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    ingestionReportHandler.exceptionCaught(ctx, new VeniceException("Exception in action"));
    ArgumentCaptor<HttpResponse> responseArgumentCaptor = ArgumentCaptor.forClass(HttpResponse.class);

    verify(ctx, times(1)).writeAndFlush(responseArgumentCaptor.capture());
    Assert
        .assertEquals(responseArgumentCaptor.getAllValues().get(0).status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
  }
}
