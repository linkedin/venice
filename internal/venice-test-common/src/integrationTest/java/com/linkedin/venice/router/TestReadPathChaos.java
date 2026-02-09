package com.linkedin.venice.router;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test to reproduce high router latency with low response_waiting_time.
 *
 * In production, we observe:
 *   - Router P99 multiget_streaming latency spikes to 1-9 seconds
 *   - Router P99 multiget_streaming response_waiting_time stays below 10ms
 *
 * This means servers respond fast, but something inside the router delays request processing.
 * The latency metric (ROUTER_SERVER_TIME) measures: completionTime - requestDecodeTime.
 * The response_waiting_time metric (ROUTER_RESPONSE_WAIT_TIME) measures:
 *   completionTime - gatherResponsesEntryTime.
 * The gap is: gatherResponsesEntryTime - requestDecodeTime = handler chain processing time.
 *
 * Key finding: the scatter-gather handler chain (handler0 -> handler1 -> gatherResponses)
 * runs INLINE on the Netty EventLoop during the I/O phase (via thenCompose, not
 * thenComposeAsync, and stageExecutor's inEventLoop() short-circuit). This means EventLoop
 * task-queue injection has no effect on the handler chain.
 *
 * To reproduce the gap, we add a custom Netty pipeline handler (via the existing
 * RouterServer.addOptionalChannelHandler API) that sits between the HTTP decoder (where
 * BasicHttpRequest._requestNanos is set) and the scatter-gather handler. When enabled,
 * this handler delays channelRead propagation using ctx.executor().schedule(), simulating
 * whatever causes the production delay (slow metadata lookups, GC pauses, etc.).
 *
 * Value size distribution:
 *   40% ~4 bytes, 20% ~2KB, 20% ~8KB, 10% ~16KB, 10% ~24KB
 */
public class TestReadPathChaos {
  private static final Logger LOGGER = LogManager.getLogger(TestReadPathChaos.class);

  private static final String STORE_NAME = "cert-histogram-dataset";
  private static final String KEY_SCHEMA = "\"int\"";
  private static final String VALUE_SCHEMA = "\"string\"";
  private static final int VALUE_SCHEMA_ID = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

  // ---- Value size buckets and record counts ----
  private static final int TOTAL_RECORDS = 50_000;
  private static final int BUCKET_0_COUNT = (int) (TOTAL_RECORDS * 0.40);
  private static final int BUCKET_1_COUNT = (int) (TOTAL_RECORDS * 0.20);
  private static final int BUCKET_2_COUNT = (int) (TOTAL_RECORDS * 0.20);
  private static final int BUCKET_3_COUNT = (int) (TOTAL_RECORDS * 0.10);
  private static final int BUCKET_4_COUNT =
      TOTAL_RECORDS - BUCKET_0_COUNT - BUCKET_1_COUNT - BUCKET_2_COUNT - BUCKET_3_COUNT;

  private static final int VALUE_SIZE_4B = 4;
  private static final int VALUE_SIZE_2KB = 2048;
  private static final int VALUE_SIZE_8KB = 8192;
  private static final int VALUE_SIZE_16KB = 16384;
  private static final int VALUE_SIZE_24KB = 24576;

  // ---- Client traffic parameters ----
  private static final int NUM_CLIENTS = 10;
  private static final int KEYS_PER_REQUEST = 100;
  private static final long TRAFFIC_DURATION_SECONDS = 5 * 60;
  private static final int MIN_REQUESTS_PER_SECOND = 48;
  private static final int MAX_REQUESTS_PER_SECOND = 120;

  // ---- Pipeline delay chaos parameters ----
  // When enabled, each inbound channelRead is delayed by CHAOS_DELAY_MS before being
  // propagated to the scatter-gather handler. This simulates the production delay between
  // HTTP decode (requestNanos set) and handler0/handler1/gatherResponses.
  private static final long CHAOS_DELAY_MS = 2000;
  // Chaos is enabled for CHAOS_ON_SECONDS, then disabled for CHAOS_OFF_SECONDS, repeating.
  private static final int CHAOS_ON_SECONDS = 3;
  private static final int CHAOS_OFF_SECONDS = 7;

  // Seeded Random for reproducible client RPS distribution across test runs
  private static final Random RPS_RANDOM = new Random(42);

  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private VeniceWriter<Object, Object, Object> veniceWriter;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;

  /**
   * Sharable Netty ChannelHandler that delays channelRead propagation when enabled.
   * Placed in the pipeline between HTTP decoder (BasicHttpRequest created with _requestNanos)
   * and the scatter-gather handler (which sets responseWaitStartNanos in gatherResponses).
   * When enabled, schedules the channelRead with a delay, creating a gap between
   * _requestNanos (already set) and responseWaitStartNanos (set later after the delay).
   */
  @ChannelHandler.Sharable
  static class ChaosDelayHandler extends ChannelInboundHandlerAdapter {
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final long delayMs;

    ChaosDelayHandler(long delayMs) {
      this.delayMs = delayMs;
    }

    void setEnabled(boolean value) {
      enabled.set(value);
    }

    boolean isEnabled() {
      return enabled.get();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (enabled.get()) {
        // Schedule delayed propagation. _requestNanos is already set (by BasicHttpServerCodec),
        // but the scatter-gather handler won't see this message until after the delay.
        ctx.executor().schedule(() -> {
          if (ctx.channel().isActive()) {
            ctx.fireChannelRead(msg);
          }
        }, delayMs, TimeUnit.MILLISECONDS);
      } else {
        ctx.fireChannelRead(msg);
      }
    }
  }

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();

    Properties extraProperties = new Properties();
    extraProperties.put(ConfigKeys.SERVER_ENABLE_PARALLEL_BATCH_GET, true);
    extraProperties.put(ConfigKeys.SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE, 100);

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(3)
        .numberOfRouters(3)
        .replicationFactor(3)
        .partitionSize(TOTAL_RECORDS / 3)
        .sslToStorageNodes(true)
        .sslToKafka(false)
        .extraProperties(extraProperties)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);

    veniceCluster.getNewStore(STORE_NAME, KEY_SCHEMA, VALUE_SCHEMA);
    veniceCluster.updateStore(
        STORE_NAME,
        new UpdateStoreQueryParams().setStorageQuotaInByte(2L * 1024 * 1024 * 1024).setReadQuotaInCU(1_000_000));

    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(STORE_NAME, false);
    storeVersionName = creationResponse.getKafkaTopic();
    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA);

    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    veniceWriter = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(storeVersionName).setKeyPayloadSerializer(keySerializer)
                .setValuePayloadSerializer(valueSerializer)
                .build());

    LOGGER.info(
        "Starting data push: {} total records across 5 value-size buckets [{}, {}, {}, {}, {}]",
        TOTAL_RECORDS,
        BUCKET_0_COUNT,
        BUCKET_1_COUNT,
        BUCKET_2_COUNT,
        BUCKET_3_COUNT,
        BUCKET_4_COUNT);

    veniceWriter.broadcastStartOfPush(new HashMap<>());
    int keyIndex = 0;

    for (int i = 0; i < BUCKET_0_COUNT; i++) {
      veniceWriter.put(keyIndex++, generateStringValue(VALUE_SIZE_4B), VALUE_SCHEMA_ID).get();
    }
    LOGGER.info("Bucket 0 (4B) push complete: {} records", BUCKET_0_COUNT);

    for (int i = 0; i < BUCKET_1_COUNT; i++) {
      veniceWriter.put(keyIndex++, generateStringValue(VALUE_SIZE_2KB), VALUE_SCHEMA_ID).get();
    }
    LOGGER.info("Bucket 1 (2KB) push complete: {} records", BUCKET_1_COUNT);

    for (int i = 0; i < BUCKET_2_COUNT; i++) {
      veniceWriter.put(keyIndex++, generateStringValue(VALUE_SIZE_8KB), VALUE_SCHEMA_ID).get();
    }
    LOGGER.info("Bucket 2 (8KB) push complete: {} records", BUCKET_2_COUNT);

    for (int i = 0; i < BUCKET_3_COUNT; i++) {
      veniceWriter.put(keyIndex++, generateStringValue(VALUE_SIZE_16KB), VALUE_SCHEMA_ID).get();
    }
    LOGGER.info("Bucket 3 (16KB) push complete: {} records", BUCKET_3_COUNT);

    for (int i = 0; i < BUCKET_4_COUNT; i++) {
      veniceWriter.put(keyIndex++, generateStringValue(VALUE_SIZE_24KB), VALUE_SCHEMA_ID).get();
    }
    LOGGER.info("Bucket 4 (24KB) push complete: {} records", BUCKET_4_COUNT);

    veniceWriter.broadcastEndOfPush(new HashMap<>());
    LOGGER.info("End of push broadcast sent, total keys written: {}", keyIndex);

    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(120, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), STORE_NAME)
          .getStore()
          .getCurrentVersion();
      return currentVersion == pushVersion;
    });
    LOGGER.info("Push version {} is now the current version for store {}", pushVersion, STORE_NAME);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  /**
   * Reproduces the production scenario where router P99 multiget_streaming latency spikes
   * while response_waiting_time stays low.
   *
   * Approach:
   * 1. Install a ChaosDelayHandler in each router's Netty pipeline (between HTTP decoder
   *    and scatter-gather handler) BEFORE clients connect.
   * 2. Start 10 clients sending high-QPS multiget streaming traffic.
   * 3. After warmup, periodically toggle chaos on (3s) and off (7s). When on, every inbound
   *    request is delayed by {@link #CHAOS_DELAY_MS} ms in the pipeline. The request's
   *    _requestNanos was already set by BasicHttpServerCodec during HTTP decode, but the
   *    scatter-gather handler doesn't process it until after the delay.
   * 4. Result: ROUTER_SERVER_TIME (latency) includes the pipeline delay,
   *    but ROUTER_RESPONSE_WAIT_TIME does not (measured from gatherResponses entry, after delay).
   *
   * Expected outcome:
   *   - Max router P99 latency >= 2 seconds (includes pipeline delay)
   *   - Max router P99 response_waiting_time stays low (servers respond fast)
   *   - Latency / response_waiting_time ratio >> 1
   */
  @Test(timeOut = 15 * 60 * Time.MS_PER_SECOND)
  public void testMultigetStreamingWithServerChaos() throws Exception {
    // Install ChaosDelayHandler on each router BEFORE clients connect.
    // The handler is added to the pipeline via addOptionalChannelHandler, which is invoked
    // during child channel initialization (for each new client connection).
    ChaosDelayHandler chaosHandler = new ChaosDelayHandler(CHAOS_DELAY_MS);
    for (VeniceRouterWrapper routerWrapper: veniceCluster.getVeniceRouters()) {
      RouterServer routerServer = routerWrapper.getRouter();
      routerServer.addOptionalChannelHandler("ChaosDelayHandler", chaosHandler);
    }
    LOGGER.info(
        "Installed ChaosDelayHandler ({}ms delay) on {} routers",
        CHAOS_DELAY_MS,
        veniceCluster.getVeniceRouters().size());

    D2Client d2Client = D2TestUtils.getD2Client(veniceCluster.getZk().getAddress(), true, HttpProtocolVersion.HTTP_1_1);
    D2TestUtils.startD2Client(d2Client);

    final AtomicLong totalClientRequests = new AtomicLong(0);
    final AtomicLong totalClientUnhealthyRequests = new AtomicLong(0);
    final List<Long> clientLatenciesMs = Collections.synchronizedList(new ArrayList<>());
    final AtomicBoolean trafficRunning = new AtomicBoolean(true);

    List<AvroGenericStoreClient<Integer, Object>> clients = new ArrayList<>();
    MetricsRepository[] clientMetricsRepos = new MetricsRepository[NUM_CLIENTS];

    try {
      for (int i = 0; i < NUM_CLIENTS; i++) {
        clientMetricsRepos[i] = new MetricsRepository();
        AvroGenericStoreClient<Integer, Object> client = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(STORE_NAME)
                .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
                .setD2Client(d2Client)
                .setMetricsRepository(clientMetricsRepos[i])
                .setUseFastAvro(false));
        clients.add(client);
      }
      LOGGER.info("Created {} thin clients", NUM_CLIENTS);

      // Warm up (chaos disabled)
      {
        Set<Integer> warmupKeys = new HashSet<>();
        for (int k = 0; k < 10; k++) {
          warmupKeys.add(k);
        }
        AvroGenericStoreClient<Integer, Object> warmupClient = clients.get(0);
        CountDownLatch warmupLatch = new CountDownLatch(1);
        AtomicReference<Exception> warmupException = new AtomicReference<>();
        warmupClient.streamingBatchGet(warmupKeys, new StreamingCallback<Integer, Object>() {
          @Override
          public void onRecordReceived(Integer key, Object value) {
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            exception.ifPresent(warmupException::set);
            warmupLatch.countDown();
          }
        });
        if (!warmupLatch.await(30, TimeUnit.SECONDS)) {
          throw new RuntimeException("Warmup streaming request timed out");
        }
        if (warmupException.get() != null) {
          throw new RuntimeException("Warmup streaming request failed", warmupException.get());
        }
        LOGGER.info("Warmup streaming request succeeded");
      }

      // Start traffic threads
      ScheduledExecutorService trafficScheduler = Executors.newScheduledThreadPool(NUM_CLIENTS);
      ExecutorService requestExecutor = Executors.newFixedThreadPool(NUM_CLIENTS * 2);
      ScheduledExecutorService metricsSampler = null;

      try {
        for (int clientIdx = 0; clientIdx < NUM_CLIENTS; clientIdx++) {
          final AvroGenericStoreClient<Integer, Object> trackingClient = clients.get(clientIdx);
          final int cIdx = clientIdx;
          int rps = MIN_REQUESTS_PER_SECOND + RPS_RANDOM.nextInt(MAX_REQUESTS_PER_SECOND - MIN_REQUESTS_PER_SECOND + 1);
          long intervalMs = 1000L / rps;

          trafficScheduler.scheduleAtFixedRate(() -> {
            if (!trafficRunning.get()) {
              return;
            }
            requestExecutor.submit(() -> {
              try {
                Set<Integer> keys = generateRandomKeys(KEYS_PER_REQUEST, TOTAL_RECORDS, ThreadLocalRandom.current());
                final int keyCount = keys.size();
                final long startNs = System.nanoTime();
                final AtomicLong recordCount = new AtomicLong(0);
                CountDownLatch latch = new CountDownLatch(1);

                trackingClient.streamingBatchGet(keys, new StreamingCallback<Integer, Object>() {
                  @Override
                  public void onRecordReceived(Integer key, Object value) {
                    recordCount.incrementAndGet();
                  }

                  @Override
                  public void onCompletion(Optional<Exception> exception) {
                    long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
                    clientLatenciesMs.add(latencyMs);
                    totalClientRequests.incrementAndGet();

                    if (exception.isPresent() || recordCount.get() < keyCount) {
                      totalClientUnhealthyRequests.incrementAndGet();
                    }
                    if (exception.isPresent()) {
                      LOGGER.warn("Client {} request completed with exception: {}", cIdx, exception.get().getMessage());
                    }
                    latch.countDown();
                  }
                });
                if (!latch.await(60, TimeUnit.SECONDS)) {
                  totalClientUnhealthyRequests.incrementAndGet();
                  totalClientRequests.incrementAndGet();
                  LOGGER.warn("Client {} request timed out after 60s", cIdx);
                }
              } catch (Exception e) {
                totalClientUnhealthyRequests.incrementAndGet();
                totalClientRequests.incrementAndGet();
                LOGGER.warn("Client {} request failed: {}", cIdx, e.getMessage());
              }
            });
          }, 0, intervalMs, TimeUnit.MILLISECONDS);
        }
        LOGGER.info(
            "Traffic started from {} clients ({}-{} rps each), running for {} seconds",
            NUM_CLIENTS,
            MIN_REQUESTS_PER_SECOND,
            MAX_REQUESTS_PER_SECOND,
            TRAFFIC_DURATION_SECONDS);

        // ------ Periodic P99 metrics sampling (every second) ------
        final AtomicReference<Double> maxRouterP99Latency = new AtomicReference<>(0.0);
        final AtomicReference<Double> maxRouterP99ResponseWaitingTime = new AtomicReference<>(0.0);
        final AtomicReference<Double> maxRouterP99PipelineLatency = new AtomicReference<>(0.0);
        final AtomicReference<Double> maxRouterP99ScatterLatency = new AtomicReference<>(0.0);
        final AtomicReference<Double> maxRouterP99QueueLatency = new AtomicReference<>(0.0);
        final AtomicReference<Double> maxRouterP99DispatchLatency = new AtomicReference<>(0.0);
        final String storeLatencyP99Key = "." + STORE_NAME + "--multiget_streaming_latency.99thPercentile";
        final String totalLatencyP99Key = ".total--multiget_streaming_latency.99thPercentile";
        final String storeWaitingP99Key =
            "." + STORE_NAME + "--multiget_streaming_response_waiting_time.99thPercentile";
        final String totalWaitingP99Key = ".total--multiget_streaming_response_waiting_time.99thPercentile";
        final String storePipelineP99Key = "." + STORE_NAME + "--multiget_streaming_pipeline_latency.99thPercentile";
        final String storeScatterP99Key = "." + STORE_NAME + "--multiget_streaming_scatter_latency.99thPercentile";
        final String storeQueueP99Key = "." + STORE_NAME + "--multiget_streaming_queue_latency.99thPercentile";
        final String storeDispatchP99Key = "." + STORE_NAME + "--multiget_streaming_dispatch_latency.99thPercentile";
        metricsSampler = Executors.newSingleThreadScheduledExecutor();
        metricsSampler.scheduleAtFixedRate(() -> {
          try {
            for (VeniceRouterWrapper router: veniceCluster.getVeniceRouters()) {
              if (!router.isRunning()) {
                continue;
              }
              MetricsRepository routerMetrics = router.getMetricsRepository();
              Map<String, ? extends Metric> metrics = routerMetrics.metrics();

              Metric storeLatencyP99 = metrics.get(storeLatencyP99Key);
              double latencyVal = storeLatencyP99 != null ? storeLatencyP99.value() : 0.0;
              double waitVal = metrics.get(storeWaitingP99Key) != null ? metrics.get(storeWaitingP99Key).value() : 0.0;
              double pipelineVal =
                  metrics.get(storePipelineP99Key) != null ? metrics.get(storePipelineP99Key).value() : 0.0;
              double scatterVal =
                  metrics.get(storeScatterP99Key) != null ? metrics.get(storeScatterP99Key).value() : 0.0;
              double queueVal = metrics.get(storeQueueP99Key) != null ? metrics.get(storeQueueP99Key).value() : 0.0;
              double dispatchVal =
                  metrics.get(storeDispatchP99Key) != null ? metrics.get(storeDispatchP99Key).value() : 0.0;

              if (storeLatencyP99 != null) {
                maxRouterP99Latency.updateAndGet(current -> Math.max(current, latencyVal));
                maxRouterP99ResponseWaitingTime.updateAndGet(current -> Math.max(current, waitVal));
                maxRouterP99PipelineLatency.updateAndGet(current -> Math.max(current, pipelineVal));
                maxRouterP99ScatterLatency.updateAndGet(current -> Math.max(current, scatterVal));
                maxRouterP99QueueLatency.updateAndGet(current -> Math.max(current, queueVal));
                maxRouterP99DispatchLatency.updateAndGet(current -> Math.max(current, dispatchVal));
                LOGGER.info(
                    "METRICS SAMPLE: Router {} chaos:{} | latency:{} wait:{} pipeline:{} scatter:{} queue:{} dispatch:{}",
                    router.getPort(),
                    chaosHandler.isEnabled() ? "ON" : "OFF",
                    String.format("%.1f", latencyVal),
                    String.format("%.1f", waitVal),
                    String.format("%.1f", pipelineVal),
                    String.format("%.1f", scatterVal),
                    String.format("%.1f", queueVal),
                    String.format("%.1f", dispatchVal));
              }

              Metric totalLatencyP99 = metrics.get(totalLatencyP99Key);
              if (totalLatencyP99 != null) {
                double val = totalLatencyP99.value();
                maxRouterP99Latency.updateAndGet(current -> Math.max(current, val));
              }

              Metric totalWaitingP99 = metrics.get(totalWaitingP99Key);
              if (totalWaitingP99 != null) {
                double val = totalWaitingP99.value();
                maxRouterP99ResponseWaitingTime.updateAndGet(current -> Math.max(current, val));
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Error sampling metrics", e);
          }
        }, 1, 1, TimeUnit.SECONDS);

        // ------ Pipeline delay chaos injection ------
        // After 30s of warmup traffic, start toggling the ChaosDelayHandler on and off.
        Thread.sleep(30 * Time.MS_PER_SECOND);
        LOGGER.info(
            "CHAOS: Starting pipeline delay injection - {}ms delay, {}s on / {}s off",
            CHAOS_DELAY_MS,
            CHAOS_ON_SECONDS,
            CHAOS_OFF_SECONDS);

        long chaosStartTime = System.currentTimeMillis();
        long chaosEndTime = chaosStartTime + (TRAFFIC_DURATION_SECONDS - 30) * Time.MS_PER_SECOND;
        int chaosCycleMs = (CHAOS_ON_SECONDS + CHAOS_OFF_SECONDS) * 1000;

        while (System.currentTimeMillis() < chaosEndTime) {
          // Enable chaos
          chaosHandler.setEnabled(true);
          LOGGER.info("CHAOS: Pipeline delay ENABLED");
          Thread.sleep(CHAOS_ON_SECONDS * Time.MS_PER_SECOND);

          // Disable chaos
          chaosHandler.setEnabled(false);
          LOGGER.info("CHAOS: Pipeline delay DISABLED");
          long sleepMs = Math.min(CHAOS_OFF_SECONDS * Time.MS_PER_SECOND, chaosEndTime - System.currentTimeMillis());
          if (sleepMs > 0) {
            Thread.sleep(sleepMs);
          }
        }

        chaosHandler.setEnabled(false);
        LOGGER.info("CHAOS: Pipeline delay injection stopped");

        // ------ Collect and log final metrics ------
        for (VeniceRouterWrapper router: veniceCluster.getVeniceRouters()) {
          if (!router.isRunning()) {
            continue;
          }
          MetricsRepository routerMetrics = router.getMetricsRepository();
          Map<String, ? extends Metric> metrics = routerMetrics.metrics();

          Metric storeLatencyP99 = metrics.get(storeLatencyP99Key);
          if (storeLatencyP99 != null) {
            double val = storeLatencyP99.value();
            maxRouterP99Latency.updateAndGet(current -> Math.max(current, val));
            LOGGER.info("FINAL: Router {} store latency P99: {} ms", router.getPort(), val);
          }
          Metric totalLatencyP99 = metrics.get(totalLatencyP99Key);
          if (totalLatencyP99 != null) {
            double val = totalLatencyP99.value();
            maxRouterP99Latency.updateAndGet(current -> Math.max(current, val));
          }
          Metric storeWaitingP99 = metrics.get(storeWaitingP99Key);
          if (storeWaitingP99 != null) {
            double val = storeWaitingP99.value();
            maxRouterP99ResponseWaitingTime.updateAndGet(current -> Math.max(current, val));
            LOGGER.info("FINAL: Router {} store response_waiting_time P99: {} ms", router.getPort(), val);
          }
          Metric totalWaitingP99 = metrics.get(totalWaitingP99Key);
          if (totalWaitingP99 != null) {
            double val = totalWaitingP99.value();
            maxRouterP99ResponseWaitingTime.updateAndGet(current -> Math.max(current, val));
          }

          // Collect new step-by-step metrics
          Metric pipelineP99 = metrics.get(storePipelineP99Key);
          if (pipelineP99 != null) {
            double val = pipelineP99.value();
            maxRouterP99PipelineLatency.updateAndGet(current -> Math.max(current, val));
            LOGGER.info("FINAL: Router {} pipeline_latency P99: {} ms", router.getPort(), val);
          }
          Metric scatterP99 = metrics.get(storeScatterP99Key);
          if (scatterP99 != null) {
            double val = scatterP99.value();
            maxRouterP99ScatterLatency.updateAndGet(current -> Math.max(current, val));
            LOGGER.info("FINAL: Router {} scatter_latency P99: {} ms", router.getPort(), val);
          }
          Metric queueP99 = metrics.get(storeQueueP99Key);
          if (queueP99 != null) {
            double val = queueP99.value();
            maxRouterP99QueueLatency.updateAndGet(current -> Math.max(current, val));
            LOGGER.info("FINAL: Router {} queue_latency P99: {} ms", router.getPort(), val);
          }
          Metric dispatchP99 = metrics.get(storeDispatchP99Key);
          if (dispatchP99 != null) {
            double val = dispatchP99.value();
            maxRouterP99DispatchLatency.updateAndGet(current -> Math.max(current, val));
            LOGGER.info("FINAL: Router {} dispatch_latency P99: {} ms", router.getPort(), val);
          }
        }

        long clientP99Latency = computePercentile(clientLatenciesMs, 99);

        LOGGER.info("======== PIPELINE DELAY CHAOS TEST RESULTS ========");
        LOGGER.info(
            "Max router P99 multiget_streaming latency (across entire test): {} ms",
            String.format("%.2f", maxRouterP99Latency.get()));
        LOGGER.info(
            "Max router P99 multiget_streaming response_waiting_time (across entire test): {} ms",
            String.format("%.2f", maxRouterP99ResponseWaitingTime.get()));
        LOGGER.info("--- Step-by-step latency breakdown (max P99 across test) ---");
        LOGGER.info(
            "  Step 1->2  pipeline_latency (Netty handlers before scatter-gather): {} ms",
            String.format("%.2f", maxRouterP99PipelineLatency.get()));
        LOGGER.info(
            "  Step 2->3  scatter_latency (scatter() call duration): {} ms",
            String.format("%.2f", maxRouterP99ScatterLatency.get()));
        LOGGER.info(
            "  Step 3->4  queue_latency (EventLoop queue wait): {} ms",
            String.format("%.2f", maxRouterP99QueueLatency.get()));
        LOGGER.info(
            "  Step 4->5  dispatch_latency (handler1 dispatch to gatherResponses): {} ms",
            String.format("%.2f", maxRouterP99DispatchLatency.get()));
        LOGGER.info("--- Client metrics ---");
        LOGGER.info("Total client requests: {}", totalClientRequests.get());
        LOGGER.info("Total client unhealthy requests: {}", totalClientUnhealthyRequests.get());
        LOGGER.info("Client-side P99 latency: {} ms", clientP99Latency);
        LOGGER.info("Client latency sample count: {}", clientLatenciesMs.size());
        if (totalClientRequests.get() > 0) {
          double unhealthyRate = (double) totalClientUnhealthyRequests.get() / totalClientRequests.get() * 100.0;
          LOGGER.info("Client unhealthy request rate: {}%", String.format("%.2f", unhealthyRate));
        }
        double latencyToWaitRatio = maxRouterP99ResponseWaitingTime.get() > 0
            ? maxRouterP99Latency.get() / maxRouterP99ResponseWaitingTime.get()
            : Double.POSITIVE_INFINITY;
        LOGGER.info(
            "Latency / response_waiting_time ratio: {} (target: >> 1, production sees 100-900x)",
            String.format("%.1f", latencyToWaitRatio));
        LOGGER.info("====================================================");

        Assert.assertTrue(totalClientRequests.get() > 0, "Should have sent at least some requests");
        Assert.assertTrue(clientLatenciesMs.size() > 0, "Should have recorded at least some latencies");
      } finally {
        // Stop traffic and shut down executors
        trafficRunning.set(false);
        LOGGER.info("Shutting down executors...");

        trafficScheduler.shutdown();
        try {
          if (!trafficScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
            LOGGER.warn("trafficScheduler did not terminate in time, forcing shutdown");
            trafficScheduler.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Interrupted while waiting for trafficScheduler termination", e);
          trafficScheduler.shutdownNow();
          Thread.currentThread().interrupt();
        }

        requestExecutor.shutdown();
        try {
          if (!requestExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
            LOGGER.warn("requestExecutor did not terminate in time, forcing shutdown");
            requestExecutor.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Interrupted while waiting for requestExecutor termination", e);
          requestExecutor.shutdownNow();
          Thread.currentThread().interrupt();
        }

        if (metricsSampler != null) {
          metricsSampler.shutdown();
          try {
            if (!metricsSampler.awaitTermination(10, TimeUnit.SECONDS)) {
              LOGGER.warn("metricsSampler did not terminate in time, forcing shutdown");
              metricsSampler.shutdownNow();
            }
          } catch (InterruptedException e) {
            LOGGER.warn("Interrupted while waiting for metricsSampler termination", e);
            metricsSampler.shutdownNow();
            Thread.currentThread().interrupt();
          }
        }

        LOGGER.info("Executors shut down complete");
      }

    } finally {
      for (AvroGenericStoreClient<Integer, Object> client: clients) {
        Utils.closeQuietlyWithErrorLogged(client);
      }
      D2ClientUtils.shutdownClient(d2Client);
    }
  }

  private static String generateStringValue(int size) {
    if (size <= 0) {
      return "";
    }
    StringBuilder sb = new StringBuilder(size);
    String pattern = "abcdefghijklmnopqrstuvwxyz0123456789";
    while (sb.length() < size) {
      int remaining = size - sb.length();
      if (remaining >= pattern.length()) {
        sb.append(pattern);
      } else {
        sb.append(pattern, 0, remaining);
      }
    }
    return sb.toString();
  }

  private static Set<Integer> generateRandomKeys(int count, int maxKey, Random rng) {
    Set<Integer> keys = new HashSet<>(count * 2);
    while (keys.size() < count) {
      keys.add(rng.nextInt(maxKey));
    }
    return keys;
  }

  private static long computePercentile(List<Long> latencies, int percentile) {
    if (latencies.isEmpty()) {
      return 0;
    }
    List<Long> sorted = new ArrayList<>(latencies);
    Collections.sort(sorted);
    int index = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
    index = Math.max(0, Math.min(index, sorted.size() - 1));
    return sorted.get(index);
  }
}
