package com.linkedin.venice.listener;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_THREAD_POOL_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.ThreadPoolOtelMetricEntity;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ServerThreadPoolSamplerTest {
  private InMemoryMetricReader reader;
  private VeniceMetricsRepository repository;
  private ThreadPoolExecutor readExecutor;
  private ThreadPoolExecutor computeExecutor;
  private ServerNettyThreadStatsHandler nettyHandler;
  private ServerThreadPoolSampler sampler;

  @BeforeMethod
  public void setUp() {
    reader = InMemoryMetricReader.create();
    repository = repository(false);
    readExecutor = mock(ThreadPoolExecutor.class);
    computeExecutor = mock(ThreadPoolExecutor.class);
    nettyHandler = mock(ServerNettyThreadStatsHandler.class);
    doReturn(96).when(readExecutor).getMaximumPoolSize();
    doReturn(300).when(computeExecutor).getMaximumPoolSize();
    sampler = new ServerThreadPoolSampler(repository, readExecutor, computeExecutor, nettyHandler, 300);
  }

  private static final MetricEntity DEFAULT_HISTOGRAM = new MetricEntity(
      "unrelated.default_histogram",
      MetricType.HISTOGRAM,
      MetricUnit.NUMBER,
      "Histogram without explicit boundaries",
      Collections.singleton(VENICE_THREAD_POOL_NAME));

  private static Collection<MetricEntity> entities() {
    List<MetricEntity> entities = new ArrayList<>(
        ModuleMetricEntityInterface
            .getUniqueMetricEntities(Collections.singletonList(ThreadPoolOtelMetricEntity.class)));
    entities.add(DEFAULT_HISTOGRAM);
    return entities;
  }

  private VeniceMetricsRepository repository(boolean applicationProvided) {
    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix("server")
            .setMetricEntities(entities())
            .setEmitOtelMetrics(true)
            .setUseOpenTelemetryInitializedByApplication(applicationProvided)
            .setOtelAdditionalMetricsReader(reader)
            .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
            .build());
  }

  @AfterMethod
  public void tearDown() {
    sampler.close();
    repository.close();
  }

  @Test
  public void testFixedDelaySamplesIdleAndBusyTimeAndCloseCancels() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    ScheduledFuture<?> future = mock(ScheduledFuture.class);
    doReturn(future).when(scheduler).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    sampler.start(scheduler);
    sampler.start(scheduler);
    verify(scheduler).scheduleWithFixedDelay(task.capture(), eq(0L), eq(100L), eq(TimeUnit.MILLISECONDS));
    for (int i = 0; i < 600; i++) {
      int count = i < 180 ? 0 : i < 570 ? 30 : 96;
      doReturn(count).when(readExecutor).getActiveCount();
      doReturn(count).when(computeExecutor).getActiveCount();
      doReturn(count).when(nettyHandler).getActiveRequestThreadCount();
      task.getValue().run();
    }
    for (String pool: new String[] { "storage_execution_thread_pool", "storage_compute_thread_pool",
        "netty_worker_thread_pool" }) {
      HistogramPointData point = point(pool);
      assertEquals(point.getCount(), 600);
      assertEquals(point.getCounts().get(0).longValue(), 180);
      assertEquals(point.getCounts().get(30).longValue(), 390);
      assertEquals(point.getCounts().get(96).longValue(), 30);
      assertEquals(point.getCounts().get(0) / (double) point.getCount(), 0.30);
      assertEquals(point.getCounts().get(96) / (double) point.getCount(), 0.05);
    }
    sampler.close();
    sampler.close();
    task.getValue().run();
    sampler.start(scheduler);
    verify(future).cancel(false);
    verify(readExecutor, times(600)).getActiveCount();
    assertEquals(point("storage_execution_thread_pool").getCount(), 600);
    verifyNoMoreInteractions(scheduler);
  }

  @Test
  public void testIntegerBinsOverrideDefaultExponentialHistograms() {
    assertTrue(repository.getVeniceMetricsConfig().useOtelExponentialHistogram());
    assertIntegerBins();
    repository.getOpenTelemetryMetricsRepository()
        .createDoubleHistogram(DEFAULT_HISTOGRAM)
        .record(31, Attributes.empty());
    assertTrue(
        reader.collectAllMetrics()
            .stream()
            .anyMatch(metric -> metric.getType() == MetricDataType.EXPONENTIAL_HISTOGRAM),
        "Unrelated histograms must retain exponential aggregation");
  }

  @Test
  public void testApplicationProvidedSdkReceivesIntegerBoundaryAdvice() {
    sampler.close();
    repository.close();
    reader = InMemoryMetricReader.create();
    GlobalOpenTelemetry.resetForTest();
    try (SdkMeterProvider provider = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
      OpenTelemetrySdk.builder().setMeterProvider(provider).buildAndRegisterGlobal();
      repository = repository(true);
      sampler = new ServerThreadPoolSampler(repository, readExecutor, computeExecutor, nettyHandler, 300);
      assertIntegerBins();
    } finally {
      GlobalOpenTelemetry.resetForTest();
    }
  }

  private void assertIntegerBins() {
    for (int count: new int[] { 0, 30, 31, 96, 300, 301 }) {
      doReturn(count).when(readExecutor).getActiveCount();
      doReturn(count).when(computeExecutor).getActiveCount();
      doReturn(count).when(nettyHandler).getActiveRequestThreadCount();
      sampler.sample();
    }
    for (String pool: new String[] { "storage_execution_thread_pool", "storage_compute_thread_pool",
        "netty_worker_thread_pool" }) {
      HistogramPointData point = point(pool);
      assertEquals(point.getCount(), 6);
      assertEquals(point.getBoundaries().size(), 301);
      for (int i = 0; i <= 300; i++) {
        assertEquals(point.getBoundaries().get(i), (double) i);
        long expected = i == 0 || i == 30 || i == 31 || i == 96 || i == 300 ? 1 : 0;
        assertEquals(point.getCounts().get(i).longValue(), expected);
      }
      assertEquals(point.getCounts().get(301).longValue(), 1);
    }
  }

  @DataProvider
  public Object[][] configuredCapacities() {
    return new Object[][] { { 96, 31, 7 }, { 512, 128, 640 }, { 8, 512, 1 } };
  }

  @Test(dataProvider = "configuredCapacities")
  public void testBucketsFollowConfiguredCapacitiesAndResizedPoolsOverflow(int read, int compute, int netty) {
    sampler.close();
    repository.close();
    reader = InMemoryMetricReader.create();
    repository = repository(false);
    doReturn(read).when(readExecutor).getMaximumPoolSize();
    doReturn(compute).when(computeExecutor).getMaximumPoolSize();
    sampler = new ServerThreadPoolSampler(repository, readExecutor, computeExecutor, nettyHandler, netty);
    int sharedCapacity = Math.max(read, compute);
    for (int i = 0; i < 3; i++) {
      doReturn(i == 0 ? 0 : sharedCapacity + i - 1).when(readExecutor).getActiveCount();
      doReturn(i == 0 ? 0 : sharedCapacity + i - 1).when(computeExecutor).getActiveCount();
      doReturn(i == 0 ? 0 : netty + i - 1).when(nettyHandler).getActiveRequestThreadCount();
      sampler.sample();
    }
    for (String pool: new String[] { "storage_execution_thread_pool", "storage_compute_thread_pool",
        "netty_worker_thread_pool" }) {
      int capacity = pool.equals("netty_worker_thread_pool") ? netty : sharedCapacity;
      HistogramPointData point = point(pool);
      assertEquals(point.getCount(), 3);
      assertEquals(point.getBoundaries().size(), capacity + 1);
      for (int i = 0; i <= capacity; i++) {
        assertEquals(point.getBoundaries().get(i), (double) i);
        assertEquals(point.getCounts().get(i).longValue(), i == 0 || i == capacity ? 1L : 0L);
      }
      assertEquals(point.getCounts().get(capacity + 1).longValue(), 1L);
    }
    doReturn(sharedCapacity + 10).when(readExecutor).getMaximumPoolSize();
    doReturn(sharedCapacity + 10).when(readExecutor).getActiveCount();
    sampler.sample();
    HistogramPointData resized = point("storage_execution_thread_pool");
    assertEquals(resized.getBoundaries().size(), sharedCapacity + 1);
    assertEquals(resized.getCounts().get(sharedCapacity + 1).longValue(), 2L);
  }

  @Test
  public void testCapacityRegistrationRejectsConflictingAndLateLayouts() {
    MetricEntity entity = ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_TIME_DISTRIBUTION.getMetricEntity();
    repository.getOpenTelemetryMetricsRepository().configureIntegerHistogramBuckets(entity, 300);
    assertThrows(
        IllegalStateException.class,
        () -> repository.getOpenTelemetryMetricsRepository().configureIntegerHistogramBuckets(entity, 301));
    assertThrows(
        IllegalArgumentException.class,
        () -> repository.getOpenTelemetryMetricsRepository().configureIntegerHistogramBuckets(entity, -1));
    MetricEntity late = new MetricEntity(
        "late.histogram",
        MetricType.HISTOGRAM,
        MetricUnit.NUMBER,
        "Late configuration",
        Collections.singleton(VENICE_THREAD_POOL_NAME),
        Collections.emptyList());
    repository.getOpenTelemetryMetricsRepository().createDoubleHistogram(late);
    assertThrows(
        IllegalStateException.class,
        () -> repository.getOpenTelemetryMetricsRepository().configureIntegerHistogramBuckets(late, 10));
  }

  @DataProvider
  public Object[][] disabledRepositories() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "disabledRepositories")
  public void testDisabledMetricsNeverScheduleOrQueryExecutors(boolean veniceRepository) {
    MetricsRepository plain = veniceRepository
        ? new VeniceMetricsRepository(
            new VeniceMetricsConfig.Builder().setEmitOtelMetrics(false)
                .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig())
                .build())
        : new MetricsRepository();
    try {
      ThreadPoolExecutor readExecutor = mock(ThreadPoolExecutor.class);
      ThreadPoolExecutor computeExecutor = mock(ThreadPoolExecutor.class);
      ServerNettyThreadStatsHandler nettyHandler = mock(ServerNettyThreadStatsHandler.class);
      ServerThreadPoolSampler disabled =
          new ServerThreadPoolSampler(plain, readExecutor, computeExecutor, nettyHandler, 300);
      ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
      disabled.start(scheduler);
      disabled.sample();
      disabled.close();
      verifyNoInteractions(scheduler, readExecutor, computeExecutor);
      verify(nettyHandler, never()).getActiveRequestThreadCount();
    } finally {
      plain.close();
    }
  }

  @Test
  public void testSchedulingFailureIsSafeToClose() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    doThrow(new RejectedExecutionException("synthetic shutdown")).when(scheduler)
        .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
    assertThrows(RejectedExecutionException.class, () -> sampler.start(scheduler));
    sampler.close();
    sampler.sample();
    sampler.start(scheduler);
    verify(scheduler).scheduleWithFixedDelay(any(), eq(0L), eq(100L), eq(TimeUnit.MILLISECONDS));
    verifyNoMoreInteractions(scheduler);
    verify(readExecutor, never()).getActiveCount();
    verify(computeExecutor, never()).getActiveCount();
  }

  @Test
  public void testFailedPoolSampleDoesNotStopOtherPoolsOrFutureSamples() {
    doThrow(new IllegalStateException("synthetic sampler failure")).doReturn(96).when(readExecutor).getActiveCount();
    sampler.sample();
    sampler.sample();
    assertEquals(point("storage_execution_thread_pool").getCount(), 1);
    assertEquals(point("storage_compute_thread_pool").getCount(), 2);
    assertEquals(point("netty_worker_thread_pool").getCount(), 2);
  }

  private HistogramPointData point(String pool) {
    ThreadPoolOtelMetricEntity entity = pool.equals("netty_worker_thread_pool")
        ? ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_TIME_DISTRIBUTION
        : ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_TIME_DISTRIBUTION;
    return OpenTelemetryDataTestUtils.getHistogramPointData(
        reader.collectAllMetrics(),
        entity.getMetricEntity().getMetricName(),
        "server",
        Attributes.builder().put(VENICE_THREAD_POOL_NAME.getDimensionNameInDefaultFormat(), pool).build());
  }
}
