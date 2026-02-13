package com.linkedin.venice.router;

import static com.linkedin.venice.VeniceConstants.DEFAULT_PER_ROUTER_READ_QUOTA;
import static com.linkedin.venice.router.httpclient.StorageNodeClientType.APACHE_HTTP_ASYNC_CLIENT;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.StatTrackingStoreClient;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.tehuti.MetricsUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestStreaming {
  private static final Logger LOGGER = LogManager.getLogger(TestStreaming.class);

  private static final int MAX_KEY_LIMIT = 1000;
  private static final int LAST_KEY_INDEX_WITH_NON_NULL_VALUE = 500;
  private static final String NON_EXISTING_KEY1 = "a_unknown_key";
  private static final String NON_EXISTING_KEY2 = "z_unknown_key";
  private static final int NON_EXISTING_KEY_NUM = 2;
  private VeniceClusterWrapper veniceCluster;
  private String storeVersionName;
  private int valueSchemaId;
  private String storeName;
  private CompressorFactory compressorFactory;
  private VeniceKafkaSerializer keySerializer;
  private VeniceKafkaSerializer valueSerializer;
  private VeniceWriter<Object, Object, Object> veniceWriter;

  private static final String KEY_PREFIX = "key_";
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "{\n" + "  \"type\": \"record\",\n"
      + "  \"name\": \"test_value_schema\",\n" + "  \"fields\": [\n"
      + "   {\"name\": \"int_field\", \"type\": \"int\"},\n" + "   {\"name\": \"float_field\", \"type\": \"float\"},\n"
      + "   {\"name\": \"nullable_string_field\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n"
      + "}";
  private static final Schema VALUE_SCHEMA_OBJECT = Schema.parse(VALUE_SCHEMA);

  @BeforeClass
  public void setUp() throws InterruptedException, ExecutionException, VeniceClientException, IOException {
    /**
     * The following config is used to detect Netty resource leaking.
     * If memory leak happens, you will see the following log message:
     *
     *  ERROR io.netty.util.ResourceLeakDetector - LEAK: ByteBuf.release() was not called before it's garbage-collected.
     *  See http://netty.io/wiki/reference-counted-objects.html for more information.
     **/

    System.setProperty("io.netty.leakDetection.maxRecords", "50");
    System.setProperty("io.netty.leakDetection.level", "paranoid");

    Utils.thisIsLocalhost();
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .numberOfRouters(0)
        .replicationFactor(2)
        .partitionSize(100)
        .sslToStorageNodes(true)
        .sslToKafka(false)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);

    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.SERVER_ENABLE_PARALLEL_BATCH_GET, true);
    serverProperties.put(ConfigKeys.SERVER_PARALLEL_BATCH_GET_CHUNK_SIZE, 100);
    veniceCluster.addVeniceServer(serverFeatureProperties, serverProperties);

    // Create test store
    CompressionStrategy compressionStrategy = CompressionStrategy.GZIP;

    storeName = Utils.getUniqueString("venice-store");
    veniceCluster.getNewStore(storeName, KEY_SCHEMA, VALUE_SCHEMA);
    VersionCreationResponse creationResponse = veniceCluster.getNewVersion(storeName, false);

    storeVersionName = creationResponse.getKafkaTopic();
    veniceCluster.updateStore(storeName, new UpdateStoreQueryParams().setReadQuotaInCU(DEFAULT_PER_ROUTER_READ_QUOTA));

    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;

    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA);

    compressorFactory = new CompressorFactory();
    VeniceCompressor compressor = compressorFactory.getCompressor(compressionStrategy);
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        veniceCluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    veniceWriter = IntegrationTestPushUtils
        .getVeniceWriterFactory(veniceCluster.getPubSubBrokerWrapper(), pubSubProducerAdapterFactory)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(storeVersionName).setKeyPayloadSerializer(keySerializer).build());

    final int pushVersion = Version.parseVersionFromKafkaTopicName(storeVersionName);

    veniceWriter.broadcastStartOfPush(false, false, compressionStrategy, new HashMap<>());
    // Insert test record and wait synchronously for it to succeed
    for (int i = 0; i < 10000; ++i) {
      GenericRecord valueRecord = new GenericData.Record(VALUE_SCHEMA_OBJECT);
      valueRecord.put("int_field", i);
      valueRecord.put("float_field", i + 100.0f);
      if (i <= LAST_KEY_INDEX_WITH_NON_NULL_VALUE) {
        valueRecord.put("nullable_string_field", "nullable_string_field" + i);
      }

      byte[] value = compressor.compress(valueSerializer.serialize("", valueRecord));
      veniceWriter.put(KEY_PREFIX + i, value, valueSchemaId).get();
    }
    // Write end of push message to make node become ONLINE from BOOTSTRAP
    veniceWriter.broadcastEndOfPush(new HashMap<>());

    // Wait for storage node to finish consuming, and new version to be activated
    String controllerUrl = veniceCluster.getAllControllersURLs();
    TestUtils.waitForNonDeterministicCompletion(30, TimeUnit.SECONDS, () -> {
      int currentVersion = ControllerClient.getStore(controllerUrl, veniceCluster.getClusterName(), storeName)
          .getStore()
          .getCurrentVersion();
      return currentVersion == pushVersion;
    });
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
    Utils.closeQuietlyWithErrorLogged(veniceWriter);
    Utils.closeQuietlyWithErrorLogged(compressorFactory);
  }

  private Properties getRouterProperties(
      boolean enableNettyClient,
      boolean enableClientCompression,
      boolean routerH2Enabled,
      int connectionLimit) {
    // To trigger long-tail retry
    Properties routerProperties = new Properties();
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_SINGLE_GET_THRESHOLD_MS, 1);
    routerProperties.put(ConfigKeys.ROUTER_MAX_KEY_COUNT_IN_MULTIGET_REQ, MAX_KEY_LIMIT); // 10 keys at most in a
                                                                                          // batch-get request
    routerProperties.put(ConfigKeys.ROUTER_LONG_TAIL_RETRY_FOR_BATCH_GET_THRESHOLD_MS, "1-:100");
    routerProperties.put(ConfigKeys.ROUTER_STORAGE_NODE_CLIENT_TYPE, APACHE_HTTP_ASYNC_CLIENT.name());
    routerProperties.put(ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED, Boolean.toString(enableClientCompression));
    routerProperties.put(ConfigKeys.ROUTER_HTTP2_INBOUND_ENABLED, Boolean.toString(routerH2Enabled));
    routerProperties.put(ConfigKeys.ROUTER_CONNECTION_LIMIT, Integer.toString(connectionLimit));

    return routerProperties;
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testReadStreaming(boolean enableRouterHttp2) throws Exception {
    // Start a new router every time with the right config
    // With Apache HAC on Router with client compression enabled
    veniceCluster.getVeniceRouters().forEach(router -> veniceCluster.removeVeniceRouter(router.getPort()));
    VeniceRouterWrapper veniceRouterWrapperWithHttpAsyncClient =
        veniceCluster.addVeniceRouter(getRouterProperties(false, true, enableRouterHttp2, 1000));
    MetricsRepository routerMetricsRepositoryWithHttpAsyncClient =
        veniceRouterWrapperWithHttpAsyncClient.getMetricsRepository();
    // With Netty Client on Router with client compression disabled
    VeniceRouterWrapper veniceRouterWrapperWithNettyClient =
        veniceCluster.addVeniceRouter(getRouterProperties(true, false, enableRouterHttp2, 1000));
    D2Client d2Client = null;
    AvroGenericStoreClient d2StoreClient = null;
    try {
      // test with D2 store client, since streaming support is only available with D2 client so far.
      d2Client = D2TestUtils.getD2Client(
          veniceCluster.getZk().getAddress(),
          true,
          enableRouterHttp2 ? HttpProtocolVersion.HTTP_2 : HttpProtocolVersion.HTTP_1_1);
      D2TestUtils.startD2Client(d2Client);
      MetricsRepository clientMetrics = new MetricsRepository();
      d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setMetricsRepository(clientMetrics)
              .setUseFastAvro(false));

      // Right now, all the streaming interfaces are still internal, and we will expose them once they are fully
      // verified.
      StatTrackingStoreClient trackingStoreClient = (StatTrackingStoreClient) d2StoreClient;

      for (boolean readComputeEnabled: new boolean[] { true, false }) {
        veniceCluster
            .updateStore(storeName, new UpdateStoreQueryParams().setReadComputationEnabled(readComputeEnabled));
        // Run multiple rounds
        int rounds = 10;
        int cur = 0;
        Set<String> keySet = new TreeSet<>();
        /**
         * {@link NON_EXISTING_KEY1}: "a_unknown_key" will be with key index: 0 internally, and we want to verify
         * whether the code could handle non-existing key with key index: 0
         */
        keySet.add(NON_EXISTING_KEY1);
        for (int i = 0; i < MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM; ++i) {
          keySet.add(KEY_PREFIX + i);
        }
        keySet.add(NON_EXISTING_KEY2);

        while (++cur <= rounds) {
          final Map<String, Object> finalMultiGetResultMap = new VeniceConcurrentHashMap<>();
          final AtomicInteger totalMultiGetResultCnt = new AtomicInteger(0);
          // Streaming batch-get
          CountDownLatch latch = new CountDownLatch(1);
          trackingStoreClient.streamingBatchGet(keySet, new StreamingCallback<String, Object>() {
            @Override
            public void onRecordReceived(String key, Object value) {
              if (value != null) {
                /**
                 * {@link java.util.concurrent.ConcurrentHashMap#put) could not take 'null' as the value.
                 */
                finalMultiGetResultMap.put(key, value);
              }
              totalMultiGetResultCnt.getAndIncrement();
            }

            @Override
            public void onCompletion(Optional<Exception> exception) {
              latch.countDown();
              if (exception.isPresent()) {
                LOGGER.info("MultiGet onCompletion invoked with Venice Exception", exception.get());
                fail("Exception: " + exception.get() + " is not expected");
              }
            }
          });
          latch.await();
          Assert.assertEquals(totalMultiGetResultCnt.get(), MAX_KEY_LIMIT);
          Assert.assertEquals(finalMultiGetResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM);
          // Verify the result
          verifyMultiGetResult(finalMultiGetResultMap);

          // test batch-get with streaming as the internal implementation
          CompletableFuture<Map<String, Object>> resultFuture = trackingStoreClient.streamingBatchGet(keySet);
          Map<String, Object> multiGetResultMap = resultFuture.get();
          // Regular batch-get API won't return non-existing keys
          Assert.assertEquals(multiGetResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM);
          verifyMultiGetResult(multiGetResultMap);
          // Test compute streaming
          AtomicInteger computeResultCnt = new AtomicInteger(0);
          Map<String, ComputeGenericRecord> finalComputeResultMap = new VeniceConcurrentHashMap<>();
          CountDownLatch computeLatch = new CountDownLatch(1);
          trackingStoreClient.compute()
              .project("int_field", "nullable_string_field")
              .streamingExecute(keySet, new StreamingCallback<String, ComputeGenericRecord>() {
                @Override
                public void onRecordReceived(String key, ComputeGenericRecord value) {
                  computeResultCnt.incrementAndGet();
                  if (value != null) {
                    finalComputeResultMap.put(key, value);
                  }
                }

                @Override
                public void onCompletion(Optional<Exception> exception) {
                  computeLatch.countDown();
                  if (exception.isPresent()) {
                    LOGGER.info("Compute onCompletion invoked with Venice Exception", exception.get());
                    fail("Exception: " + exception.get() + " is not expected");
                  }
                }
              });
          computeLatch.await();
          Assert.assertEquals(computeResultCnt.get(), MAX_KEY_LIMIT);
          Assert.assertEquals(finalComputeResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM); // Without
                                                                                                   // non-existing
                                                                                                   // key
          verifyComputeResult(finalComputeResultMap);
          // Test compute with streaming implementation
          CompletableFuture<VeniceResponseMap<String, ComputeGenericRecord>> computeFuture =
              trackingStoreClient.compute().project("int_field", "nullable_string_field").streamingExecute(keySet);
          Map<String, ComputeGenericRecord> computeResultMap = computeFuture.get();
          Assert.assertEquals(computeResultMap.size(), MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM);
          verifyComputeResult(computeResultMap);
        }
        // Verify some client-side metrics, and we could add verification for more metrics if necessary
        String metricPrefix = "." + storeName;
        Map<String, ? extends Metric> metrics = clientMetrics.metrics();
        Assert.assertTrue(metrics.get(metricPrefix + "--multiget_streaming_request.OccurrenceRate").value() > 0);
        Assert.assertTrue(metrics.get(metricPrefix + "--multiget_streaming_healthy_request_latency.Avg").value() > 0);
        Assert.assertTrue(metrics.get(metricPrefix + "--multiget_streaming_response_ttfr.50thPercentile").value() > 0);
        Assert
            .assertTrue(metrics.get(metricPrefix + "--multiget_streaming_response_tt50pr.50thPercentile").value() > 0);
        Assert
            .assertTrue(metrics.get(metricPrefix + "--multiget_streaming_response_tt90pr.50thPercentile").value() > 0);
        Assert
            .assertTrue(metrics.get(metricPrefix + "--multiget_streaming_healthy_request.OccurrenceRate").value() > 0);
        Assert.assertTrue(metrics.get(metricPrefix + "--compute_streaming_request.OccurrenceRate").value() > 0);
        Assert.assertTrue(metrics.get(metricPrefix + "--compute_streaming_healthy_request_latency.Avg").value() > 0);
        Assert.assertTrue(metrics.get(metricPrefix + "--compute_streaming_response_ttfr.50thPercentile").value() > 0);
        Assert.assertTrue(metrics.get(metricPrefix + "--compute_streaming_response_tt50pr.50thPercentile").value() > 0);
        Assert.assertTrue(metrics.get(metricPrefix + "--compute_streaming_response_tt90pr.50thPercentile").value() > 0);

        if (readComputeEnabled) {
          Assert.assertEquals(
              metrics.get(metricPrefix + "--compute_streaming_multiget_fallback.OccurrenceRate").value(),
              0.0);
        } else {
          Assert.assertTrue(
              metrics.get(metricPrefix + "--compute_streaming_multiget_fallback.OccurrenceRate").value() > 0);
        }

        LOGGER.info("The following metrics are Router metrics:");
        // Verify some router metrics
        for (MetricsRepository routerMetricsRepository: Collections
            .singletonList(routerMetricsRepositoryWithHttpAsyncClient)) {
          Map<String, ? extends Metric> routerMetrics = routerMetricsRepository.metrics();
          // The following metrics are only available when Router is running in Streaming mode.
          Assert
              .assertTrue(routerMetrics.get(metricPrefix + "--multiget_streaming_request.OccurrenceRate").value() > 0);
          Assert
              .assertTrue(routerMetrics.get(metricPrefix + "--multiget_streaming_latency.99thPercentile").value() > 0);
          Assert.assertTrue(
              routerMetrics.get(metricPrefix + "--multiget_streaming_fanout_request_count.Avg").value() > 0);
          if (readComputeEnabled) {
            Assert
                .assertTrue(routerMetrics.get(metricPrefix + "--compute_streaming_request.OccurrenceRate").value() > 0);
            Assert
                .assertTrue(routerMetrics.get(metricPrefix + "--compute_streaming_latency.99thPercentile").value() > 0);
            Assert.assertTrue(
                routerMetrics.get(metricPrefix + "--compute_streaming_fanout_request_count.Avg").value() > 0);
            Assert.assertTrue(
                getMaxServerMetricValue(".total--compute_storage_engine_read_compute_efficiency.Max") > 1.0);
            Assert.assertEquals(getAggregateRouterMetricValue(".total--compute_multiget_fallback.Total"), 0.0);
          } else {
            Assert.assertEquals(
                getAggregateRouterMetricValue(".total--compute_multiget_fallback.Total"),
                (double) MAX_KEY_LIMIT);
          }
        }
      }
    } finally {
      Utils.closeQuietlyWithErrorLogged(veniceRouterWrapperWithHttpAsyncClient);
      Utils.closeQuietlyWithErrorLogged(veniceRouterWrapperWithNettyClient);
      Utils.closeQuietlyWithErrorLogged(d2StoreClient);
      if (d2Client != null) {
        D2ClientUtils.shutdownClient(d2Client);
      }
    }
  }

  private double getAggregateRouterMetricValue(String metricName) {
    return MetricsUtils.getSum(metricName, veniceCluster.getVeniceRouters());
  }

  private double getMaxServerMetricValue(String metricName) {
    return MetricsUtils.getMax(metricName, veniceCluster.getVeniceServers());
  }

  private void verifyMultiGetResult(Map<String, Object> resultMap) {
    for (int i = 0; i < MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM; ++i) {
      String key = KEY_PREFIX + i;
      Object value = resultMap.get(key);
      Assert.assertTrue(value instanceof GenericRecord);
      GenericRecord record = (GenericRecord) value;
      Assert.assertEquals(record.get("int_field"), i);
      Assert.assertEquals(record.get("float_field"), i + 100.0f);
    }
  }

  private void verifyComputeResult(Map<String, ComputeGenericRecord> resultMap) {
    for (int i = 0; i < MAX_KEY_LIMIT - NON_EXISTING_KEY_NUM; ++i) {
      String key = KEY_PREFIX + i;
      GenericRecord record = resultMap.get(key);
      Assert.assertEquals(record.get("int_field"), i);
      TestUtils.checkMissingFieldInAvroRecord(record, "float_field");
      if (i <= LAST_KEY_INDEX_WITH_NON_NULL_VALUE) {
        Assert.assertEquals("nullable_string_field" + i, record.get("nullable_string_field").toString());
      } else {
        Assert.assertNull(
            record.get("nullable_string_field"),
            "Field: 'nullable_string_field' should be 'null' for key: " + key);
      }
    }
  }

  @Test(timeOut = 30000)
  public void testWithNonExistingStore() throws ExecutionException, InterruptedException {
    String nonExistingStoreName = Utils.getUniqueString("non_existing_store");
    D2Client d2Client = null;
    AvroGenericStoreClient d2StoreClient = null;
    VeniceRouterWrapper veniceRouterWrapperWithHttpAsyncClient = null;
    try {
      veniceRouterWrapperWithHttpAsyncClient = veniceCluster.addVeniceRouter(new Properties());
      d2Client = D2TestUtils.getD2Client(veniceCluster.getZk().getAddress(), false);
      D2TestUtils.startD2Client(d2Client);
      d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(nonExistingStoreName)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client));
      d2StoreClient.get("test").get();
      fail("An exception is expected here");
    } catch (ServiceDiscoveryException e) {
      assertTrue(e.getCause() instanceof VeniceNoStoreException);
    } catch (Throwable t) {
      fail("Unexpected exception received: " + t.getClass());
    } finally {
      Utils.closeQuietlyWithErrorLogged(d2StoreClient);
      Utils.closeQuietlyWithErrorLogged(veniceRouterWrapperWithHttpAsyncClient);
      if (d2Client != null) {
        D2ClientUtils.shutdownClient(d2Client);
      }
    }
  }

  @Test(timeOut = 30000)
  public void testWithForceClusterDiscovery() {
    D2Client d2Client = null;
    AvroGenericStoreClient d2StoreClient = null;
    VeniceRouterWrapper veniceRouterWrapperWithHttpAsyncClient = null;
    try {
      veniceRouterWrapperWithHttpAsyncClient = veniceCluster.addVeniceRouter(new Properties());
      d2Client = D2TestUtils.getD2Client(veniceCluster.getZk().getAddress(), false);
      // Don't start d2 client
      d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setForceClusterDiscoveryAtStartTime(true));
      fail("An exception is expected here");
    } catch (Throwable t) {
      if (!(t instanceof VeniceClientException) || !t.getMessage().contains("Failed to find d2 service")) {
        fail("Unexpected exception received: " + t.getClass() + " with message: " + t.getMessage());
      }
    } finally {
      Utils.closeQuietlyWithErrorLogged(d2StoreClient);
      Utils.closeQuietlyWithErrorLogged(veniceRouterWrapperWithHttpAsyncClient);
    }
  }

  @Test(timeOut = 600 * Time.MS_PER_SECOND)
  public void testConnectionLimitRejection() throws Exception {
    // Clear existing routers first
    veniceCluster.getVeniceRouters().forEach(router -> veniceCluster.removeVeniceRouter(router.getPort()));
    // Add a normal router
    veniceCluster.addVeniceRouter(getRouterProperties(false, true, false, 1000));
    D2Client d2Client = null;
    AvroGenericStoreClient d2StoreClient = null;
    try {
      // test with D2 store client, since streaming support is only available with D2 client so far.
      d2Client = D2TestUtils.getD2Client(veniceCluster.getZk().getAddress(), true, HttpProtocolVersion.HTTP_1_1);
      D2TestUtils.startD2Client(d2Client);
      MetricsRepository clientMetrics = new MetricsRepository();
      d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setMetricsRepository(clientMetrics)
              .setUseFastAvro(false));

      // Right now, all the streaming interfaces are still internal, and we will expose them once they are fully
      // verified.
      StatTrackingStoreClient trackingStoreClient = (StatTrackingStoreClient) d2StoreClient;

      // After the client is started successfully, clear existing routers again
      veniceCluster.getVeniceRouters().forEach(router -> veniceCluster.removeVeniceRouter(router.getPort()));
      // Add a router which rejects all connections
      VeniceRouterWrapper veniceRouterRejectsAllConnections =
          veniceCluster.addVeniceRouter(getRouterProperties(false, true, false, 0));
      MetricsRepository routerMetricsRepositoryWithHttpAsyncClient =
          veniceRouterRejectsAllConnections.getMetricsRepository();
      Set<String> keySet = new TreeSet<>();
      for (int i = 0; i < MAX_KEY_LIMIT; ++i) {
        keySet.add(KEY_PREFIX + i);
      }

      final List<String> errorMessagesFromRouter = new LinkedList<>();
      // Streaming batch-get
      CountDownLatch latch = new CountDownLatch(1);
      trackingStoreClient.streamingBatchGet(keySet, new StreamingCallback<String, Object>() {
        @Override
        public void onRecordReceived(String key, Object value) {
          // No-op
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          if (exception.isPresent()) {
            LOGGER.info("MultiGet onCompletion invoked with Venice Exception", exception.get());
            errorMessagesFromRouter.add(exception.get().getMessage());
          }
          latch.countDown();
        }
      });
      latch.await();
      Assert.assertFalse(errorMessagesFromRouter.isEmpty());
      // Verify that the error message contains 429
      Assert.assertTrue(errorMessagesFromRouter.get(0).contains("429"));

      // Verify some client-side metrics, and we could add verification for more metrics if necessary
      String metricPrefix = "." + storeName;
      Map<String, ? extends Metric> metrics = clientMetrics.metrics();
      Assert.assertTrue(metrics.get(metricPrefix + "--multiget_streaming_request.OccurrenceRate").value() > 0);
      Assert
          .assertTrue(metrics.get(metricPrefix + "--multiget_streaming_unhealthy_request.OccurrenceRate").value() > 0);

      // Verify some router metrics
      Map<String, ? extends Metric> routerMetrics = routerMetricsRepositoryWithHttpAsyncClient.metrics();
      double maxValue = routerMetrics.get(".security--connection_count.Max").value();
      double avgValue = routerMetrics.get(".security--connection_count.Avg").value();
      // Since connection limit is 0, the active connection counter would at most be incremented to 1
      Assert.assertTrue(Math.abs(maxValue - 1d) < 0.0001d);
      Assert.assertTrue(avgValue > 0 && avgValue < 1.0d);
      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.MINUTES, () -> {
        Map<String, ? extends Metric> latestRouterMetrics = routerMetricsRepositoryWithHttpAsyncClient.metrics();
        double rejectedConnectionCount = latestRouterMetrics.get(".security--rejected_connection_count.Rate").value();
        Assert.assertTrue(rejectedConnectionCount > 0);
      });
      // Gauge metric might not be updated yet since it's only updated one time each one-minute time window
      // Assert.assertTrue(routerMetrics.get(".security--connection_count_gauge.Gauge").value() > 0);
    } finally {
      // Clear all routers
      veniceCluster.getVeniceRouters().forEach(router -> veniceCluster.removeVeniceRouter(router.getPort()));
      Utils.closeQuietlyWithErrorLogged(d2StoreClient);
      if (d2Client != null) {
        D2ClientUtils.shutdownClient(d2Client);
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testResponseAggregationThreadPool() throws Exception {
    // Clear existing routers first
    veniceCluster.getVeniceRouters().forEach(router -> veniceCluster.removeVeniceRouter(router.getPort()));

    // Start a router with custom response aggregation thread pool config
    Properties routerProperties = getRouterProperties(false, true, false, 1000);
    routerProperties.put(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_THREAD_POOL_SIZE, "3");
    routerProperties.put(ConfigKeys.ROUTER_RESPONSE_AGGREGATION_QUEUE_CAPACITY, "1000");
    VeniceRouterWrapper veniceRouter = veniceCluster.addVeniceRouter(routerProperties);
    MetricsRepository routerMetricsRepository = veniceRouter.getMetricsRepository();

    D2Client d2Client = null;
    AvroGenericStoreClient<String, GenericRecord> d2StoreClient = null;

    try {
      // Setup D2 client for multiget streaming
      d2Client = D2TestUtils.getD2Client(veniceCluster.getZk().getAddress(), false, HttpProtocolVersion.HTTP_1_1);
      D2TestUtils.startD2Client(d2Client);
      MetricsRepository clientMetrics = new MetricsRepository();
      d2StoreClient = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName)
              .setD2ServiceName(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME)
              .setD2Client(d2Client)
              .setMetricsRepository(clientMetrics)
              .setUseFastAvro(false));

      // Prepare keys for multiget streaming
      Set<String> keySet = new TreeSet<>();
      for (int i = 0; i < 100; i++) {
        keySet.add(KEY_PREFIX + i);
      }

      StatTrackingStoreClient trackingStoreClient = (StatTrackingStoreClient) d2StoreClient;

      // Execute multiple multiget streaming requests to exercise the thread pool
      for (int iteration = 0; iteration < 10; iteration++) {
        final Map<String, Object> resultMap = new VeniceConcurrentHashMap<>();
        final AtomicInteger totalResultCnt = new AtomicInteger(0);

        CountDownLatch latch = new CountDownLatch(1);
        trackingStoreClient.streamingBatchGet(keySet, new StreamingCallback<String, Object>() {
          @Override
          public void onRecordReceived(String key, Object value) {
            if (value != null) {
              resultMap.put(key, value);
            }
            totalResultCnt.getAndIncrement();
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            latch.countDown();
            if (exception.isPresent()) {
              LOGGER.error("MultiGet onCompletion invoked with exception", exception.get());
              fail("Exception during multiget: " + exception.get());
            }
          }
        });
        latch.await();

        // Verify we got expected count
        Assert.assertEquals(totalResultCnt.get(), 100, "Should receive 100 records");

        // Verify we got expected data (only check the 100 keys we actually fetched)
        for (int i = 0; i < 100; i++) {
          String key = KEY_PREFIX + i;
          GenericRecord record = (GenericRecord) resultMap.get(key);
          Assert.assertNotNull(record, "Record for key " + key + " should not be null");
          Assert.assertEquals(record.get("int_field"), i);
        }
      }

      // Verify response aggregation thread pool metrics exist and show activity
      Map<String, ? extends Metric> routerMetrics = routerMetricsRepository.metrics();

      // Verify thread pool stats are present (using LambdaStat instead of Gauge)
      Assert.assertNotNull(
          routerMetrics.get(".response_aggregation_thread_pool--active_thread_number.LambdaStat"),
          "Response aggregation thread pool active_thread_number metric should exist");
      Assert.assertNotNull(
          routerMetrics.get(".response_aggregation_thread_pool--max_thread_number.LambdaStat"),
          "Response aggregation thread pool max_thread_number metric should exist");
      Assert.assertNotNull(
          routerMetrics.get(".response_aggregation_thread_pool--queued_task_count_gauge.LambdaStat"),
          "Response aggregation thread pool queued_task_count_gauge metric should exist");

      // Verify pool size matches config
      double poolSize = routerMetrics.get(".response_aggregation_thread_pool--max_thread_number.LambdaStat").value();
      Assert.assertEquals(poolSize, 3.0, "Thread pool size should match configured value");

      // Verify the thread pool metrics indicate queue/task activity is being tracked
      Metric queuedTaskMetric =
          routerMetrics.get(".response_aggregation_thread_pool--queued_task_count_gauge.LambdaStat");
      if (queuedTaskMetric != null) {
        double queuedTaskCount = queuedTaskMetric.value();
        Assert.assertTrue(
            queuedTaskCount >= 0,
            "Thread pool queued_task_count_gauge metric should be non-negative, value: " + queuedTaskCount);
      }

      LOGGER.info("Response aggregation thread pool metrics verified successfully");

    } finally {
      Utils.closeQuietlyWithErrorLogged(d2StoreClient);
      if (d2Client != null) {
        D2ClientUtils.shutdownClient(d2Client);
      }
      veniceCluster.getVeniceRouters().forEach(router -> veniceCluster.removeVeniceRouter(router.getPort()));
    }
  }
}
