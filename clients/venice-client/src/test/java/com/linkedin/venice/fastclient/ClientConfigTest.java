package com.linkedin.venice.fastclient;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.fastclient.stats.FastClientMetricEntity.REQUEST_REJECTION_COUNT;
import static com.linkedin.venice.fastclient.stats.FastClientMetricEntity.REQUEST_REJECTION_RATIO;
import static com.linkedin.venice.read.RequestType.MULTI_GET;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.RejectionReason.NO_REPLICAS_AVAILABLE;
import static com.linkedin.venice.stats.dimensions.RejectionReason.THROTTLED_BY_LOAD_CONTROLLER;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_REJECTION_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.stats.FastClientMetricEntity;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class ClientConfigTest {
  private ClientConfig.ClientConfigBuilder getClientConfigWithMinimumRequiredInputs() {
    return new ClientConfig.ClientConfigBuilder<>().setStoreName("test_store")
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_server_discovery");
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "storeName param shouldn't be empty")
  public void testClientWithNoStoreName() {
    new ClientConfig.ClientConfigBuilder<>().build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "storeName param shouldn't be empty")
  public void testClientWithEmptyStoreName() {
    new ClientConfig.ClientConfigBuilder<>().setStoreName("").build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "r2Client param shouldn't be null")
  public void testClientWithoutR2Client() {
    new ClientConfig.ClientConfigBuilder<>().setStoreName("test_store").build();
  }

  @Test
  public void testClientWithAllRequiredInputs() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.build();
  }

  @Test
  public void testRouteMetricsDisabledByDefault() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    assertTrue(clientConfigBuilder.build().isRouteMetricsDisabled());
    assertTrue(clientConfigBuilder.clone().build().isRouteMetricsDisabled());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDisableRouteMetrics(boolean disableRouteMetrics) {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        getClientConfigWithMinimumRequiredInputs().setDisableRouteMetrics(disableRouteMetrics);
    assertEquals(clientConfigBuilder.build().isRouteMetricsDisabled(), disableRouteMetrics);
    assertEquals(clientConfigBuilder.clone().build().isRouteMetricsDisabled(), disableRouteMetrics);
  }

  @Test
  public void testFeatureMetricsDisabledByDefault() {
    MetricsRepository repository = new MetricsRepository();
    MetricsRepository legacyRepository = new MetricsRepository();
    try {
      ClientConfig config = getClientConfigWithMinimumRequiredInputs().setMetricsRepository(repository)
          .setInstanceHealthMonitor(mock(InstanceHealthMonitor.class))
          .build();
      assertFalse(config.isDualReadEnabled());
      assertFalse(config.isStoreLoadControllerEnabled());

      for (RequestType requestType: RequestType.values()) {
        FastClientStats.getClientStats(legacyRepository, "", config.getStoreName(), requestType);
        assertFeatureMetricRegistration(repository, requestType, false, false, true);
      }
      Set<String> omittedMetrics = new HashSet<>(legacyRepository.metrics().keySet());
      omittedMetrics.removeAll(repository.metrics().keySet());
      assertEquals(omittedMetrics.size(), 65);
      assertTrue(
          omittedMetrics.stream()
              .allMatch(
                  name -> name.contains("dual_read_") || name.contains("rejected_request_count_by_load_controller")
                      || name.contains("rejection_ratio.")),
          "Only dual-read and store-load-controller metrics should be omitted");
    } finally {
      repository.close();
      legacyRepository.close();
    }
  }

  @Test(dataProvider = "Four-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testFeatureMetricsFollowClientConfig(
      boolean dualReadEnabled,
      boolean storeLoadControllerEnabled,
      boolean emitTehutiMetrics,
      boolean emitOtelMetrics) throws IOException {
    Set<MetricEntity> metricEntities = new HashSet<>(CLIENT_METRIC_ENTITIES);
    for (FastClientMetricEntity entity: FastClientMetricEntity.values()) {
      metricEntities.add(entity.getMetricEntity());
    }
    try (InMemoryMetricReader reader = InMemoryMetricReader.create();
        VeniceMetricsRepository repository = new VeniceMetricsRepository(
            new VeniceMetricsConfig.Builder().setServiceName(FAST_CLIENT.getName())
                .setMetricPrefix(FAST_CLIENT.getMetricsPrefix())
                .setMetricEntities(metricEntities)
                .emitTehutiMetrics(emitTehutiMetrics)
                .setEmitOtelMetrics(emitOtelMetrics)
                .setOtelAdditionalMetricsReader(reader)
                .build())) {
      ClientConfig config = getClientConfigWithMinimumRequiredInputs().setMetricsRepository(repository)
          .setInstanceHealthMonitor(mock(InstanceHealthMonitor.class))
          .setDualReadEnabled(dualReadEnabled)
          .setGenericThinClient(dualReadEnabled ? mock(AvroGenericStoreClient.class) : null)
          .setStoreLoadControllerEnabled(storeLoadControllerEnabled)
          .build();
      for (RequestType requestType: RequestType.values()) {
        assertFeatureMetricRegistration(
            repository,
            requestType,
            dualReadEnabled,
            storeLoadControllerEnabled,
            emitTehutiMetrics);
      }

      for (String clusterName: new String[] { OpenTelemetryMetricsSetup.UNKNOWN_CLUSTER_NAME, "migrated_cluster" }) {
        config.onClusterNameUpdated(clusterName);
        for (RequestType requestType: RequestType.values()) {
          FastClientStats stats = config.getStats(requestType);
          stats.emitHealthyRequestMetricsNonDavinciClient(1, 1, 1);
          stats.recordFastClientSlowerRequest();
          stats.recordFastClientErrorThinClientSucceedRequest();
          stats.recordThinClientFastClientLatencyDelta(8);
          stats.recordRejectionRatio(0);
          stats.recordRejectionRatio(0.25);
          stats.recordRejectedRequestByLoadController();
          stats.recordNoAvailableReplicaRequest();

          assertFeatureMetricRegistration(
              repository,
              requestType,
              dualReadEnabled,
              storeLoadControllerEnabled,
              emitTehutiMetrics);
          String prefix = ".test_store--" + requestType.getMetricPrefix();
          if (emitTehutiMetrics) {
            assertTrue(repository.getMetric(prefix + "request.OccurrenceRate").value() > 0);
            assertTrue(repository.getMetric(prefix + "no_available_replica_request_count.OccurrenceRate").value() > 0);
            if (dualReadEnabled) {
              assertTrue(
                  repository.getMetric(prefix + "dual_read_fastclient_slower_request_count.OccurrenceRate")
                      .value() > 0);
              assertTrue(
                  repository
                      .getMetric(prefix + "dual_read_fastclient_error_thinclient_succeed_request_count.OccurrenceRate")
                      .value() > 0);
              assertEquals(
                  repository.getMetric(prefix + "dual_read_thinclient_fastclient_latency_delta.Max").value(),
                  8.0);
            }
            if (storeLoadControllerEnabled) {
              assertTrue(
                  repository.getMetric(prefix + "rejected_request_count_by_load_controller.OccurrenceRate")
                      .value() > 0);
              assertEquals(repository.getMetric(prefix + "rejection_ratio.Max").value(), 0.25);
            }
          }
          if (emitOtelMetrics) {
            Attributes attributes =
                new OpenTelemetryDataTestUtils.OpenTelemetryAttributesBuilder().setStoreName(config.getStoreName())
                    .setRequestType(requestType)
                    .setClusterName(clusterName)
                    .build();
            String rejectionReason = VENICE_REQUEST_REJECTION_REASON.getDimensionNameInDefaultFormat();
            validateLongPointDataFromCounter(
                reader,
                1,
                attributes.toBuilder().put(rejectionReason, NO_REPLICAS_AVAILABLE.getDimensionValue()).build(),
                REQUEST_REJECTION_COUNT.getMetricEntity().getMetricName(),
                FAST_CLIENT.getMetricsPrefix());
            if (storeLoadControllerEnabled) {
              Attributes loadControllerAttributes =
                  attributes.toBuilder().put(rejectionReason, THROTTLED_BY_LOAD_CONTROLLER.getDimensionValue()).build();
              validateLongPointDataFromCounter(
                  reader,
                  1,
                  loadControllerAttributes,
                  REQUEST_REJECTION_COUNT.getMetricEntity().getMetricName(),
                  FAST_CLIENT.getMetricsPrefix());
              validateHistogramPointData(
                  reader,
                  0,
                  0.25,
                  2,
                  0.25,
                  loadControllerAttributes,
                  REQUEST_REJECTION_RATIO.getMetricEntity().getMetricName(),
                  FAST_CLIENT.getMetricsPrefix());
            }
          }
        }
        Collection<MetricData> otelMetrics = reader.collectAllMetrics();
        if (emitOtelMetrics) {
          AttributeKey<String> rejectionReasonKey =
              AttributeKey.stringKey(VENICE_REQUEST_REJECTION_REASON.getDimensionNameInDefaultFormat());
          assertEquals(
              otelMetrics.stream().anyMatch(metric -> metric.getName().endsWith(".request.rejection_ratio")),
              storeLoadControllerEnabled);
          assertEquals(
              otelMetrics.stream()
                  .filter(metric -> metric.getName().endsWith(".request.rejection_count"))
                  .flatMap(metric -> metric.getLongSumData().getPoints().stream())
                  .anyMatch(
                      point -> THROTTLED_BY_LOAD_CONTROLLER.getDimensionValue()
                          .equals(point.getAttributes().get(rejectionReasonKey))),
              storeLoadControllerEnabled,
              "Disabled load control must not emit its rejection reason on the shared counter");
        } else {
          assertTrue(otelMetrics.isEmpty());
        }
      }
    }
  }

  private void assertFeatureMetricRegistration(
      MetricsRepository repository,
      RequestType requestType,
      boolean dualReadEnabled,
      boolean storeLoadControllerEnabled,
      boolean emitTehutiMetrics) {
    String prefix = ".test_store--" + requestType.getMetricPrefix();
    Set<String> metrics = repository.metrics().keySet();
    assertEquals(
        metrics.stream().filter(name -> name.startsWith(prefix + "dual_read_")).count(),
        emitTehutiMetrics && dualReadEnabled ? 10L : 0L);
    for (String suffix: new String[] { "rejected_request_count_by_load_controller.OccurrenceRate",
        "rejection_ratio.Avg", "rejection_ratio.Max" }) {
      assertEquals(metrics.contains(prefix + suffix), emitTehutiMetrics && storeLoadControllerEnabled, prefix + suffix);
    }
    assertEquals(metrics.contains(prefix + "request.OccurrenceRate"), emitTehutiMetrics);
    assertEquals(metrics.contains(prefix + "no_available_replica_request_count.OccurrenceRate"), emitTehutiMetrics);
    if (!emitTehutiMetrics) {
      assertTrue(metrics.isEmpty());
    }
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Either param: specificThinClient or param: genericThinClient.*")
  public void testClientWithDualReadAndNoThinClients() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setDualReadEnabled(true);
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Both param: specificThinClient and param: genericThinClient should not be specified.*")
  public void testClientWithOutDualReadButWithThinClients() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setGenericThinClient(mock(AvroGenericStoreClient.class));
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "longTailRetryThresholdForSingleGetInMicroSeconds must be positive.*")
  public void testClientWithInvalidLongTailRetryThresholdForSingleGet() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true);
    clientConfigBuilder.setLongTailRetryThresholdForSingleGetInMicroSeconds(0);
    clientConfigBuilder.build();
  }

  @Test
  public void testLongTailRetryWithDualRead() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setDualReadEnabled(true)
        .setGenericThinClient(mock(AvroGenericStoreClient.class))
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingleGetInMicroSeconds(1000)
        .build();
  }

  @Test
  public void testDefaultBatchGetRetryThresholds() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    ClientConfig clientConfig = clientConfigBuilder.build();
    assertEquals(
        clientConfig.getLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds(),
        "1-12:8,13-20:30,21-150:50,151-500:100,501-:500");
  }

  @Test
  public void testDefaulComputeRetryThresholds() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    ClientConfig clientConfig = clientConfigBuilder.build();
    assertEquals(
        clientConfig.getLongTailRangeBasedRetryThresholdForComputeInMilliSeconds(),
        "1-12:8,13-20:30,21-150:50,151-500:100,501-:500");
  }

  @Test
  public void testClientConfigWithCustomKeySerializerFactory() {
    SerializerFactory mockSerializerFactory = mock(SerializerFactory.class);
    RecordSerializer mockSerializer = mock(RecordSerializer.class);
    Schema mockSchema = mock(Schema.class);
    when(mockSerializerFactory.createSerializer(mockSchema)).thenReturn(mockSerializer);

    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setKeySerializerFactory(mockSerializerFactory);
    ClientConfig clientConfig = clientConfigBuilder.build();

    // Verify the factory is present
    assertTrue(clientConfig.getKeySerializerFactory().isPresent());
    assertEquals(clientConfig.getKeySerializerFactory().get(), mockSerializerFactory);

    // Verify the factory works
    Optional<SerializerFactory> factoryOptional = clientConfig.getKeySerializerFactory();
    assertTrue(factoryOptional.isPresent());
    RecordSerializer serializer = factoryOptional.get().createSerializer(mockSchema);
    assertEquals(serializer, mockSerializer);
  }

  @Test
  public void testClientConfigWithCustomValueDeserializerFactory() {
    DeserializerFactory mockDeserializerFactory = mock(DeserializerFactory.class);
    RecordDeserializer mockDeserializer = mock(RecordDeserializer.class);
    Schema writerSchema = mock(Schema.class);
    when(mockDeserializerFactory.createDeserializer(writerSchema)).thenReturn(mockDeserializer);

    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setValueDeserializerFactory(mockDeserializerFactory);
    ClientConfig clientConfig = clientConfigBuilder.build();

    // Verify the factory is present
    assertTrue(clientConfig.getValueDeserializerFactory().isPresent());
    assertEquals(clientConfig.getValueDeserializerFactory().get(), mockDeserializerFactory);

    // Verify the factory works
    Optional<DeserializerFactory> factoryOptional = clientConfig.getValueDeserializerFactory();
    assertTrue(factoryOptional.isPresent());
    RecordDeserializer deserializer = factoryOptional.get().createDeserializer(writerSchema);
    assertEquals(deserializer, mockDeserializer);
  }

  @Test
  public void testClientConfigWithBothCustomFactories() {
    SerializerFactory mockSerializerFactory = mock(SerializerFactory.class);
    DeserializerFactory mockDeserializerFactory = mock(DeserializerFactory.class);

    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setKeySerializerFactory(mockSerializerFactory);
    clientConfigBuilder.setValueDeserializerFactory(mockDeserializerFactory);
    ClientConfig clientConfig = clientConfigBuilder.build();

    // Verify both factories are present
    assertTrue(clientConfig.getKeySerializerFactory().isPresent());
    assertTrue(clientConfig.getValueDeserializerFactory().isPresent());
    assertEquals(clientConfig.getKeySerializerFactory().get(), mockSerializerFactory);
    assertEquals(clientConfig.getValueDeserializerFactory().get(), mockDeserializerFactory);
  }

  @Test
  public void testClientConfigWithoutCustomFactories() {
    // When no custom factories are set, they should be empty optionals
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    ClientConfig clientConfig = clientConfigBuilder.build();

    assertFalse(clientConfig.getKeySerializerFactory().isPresent());
    assertFalse(clientConfig.getValueDeserializerFactory().isPresent());
  }

  // -------- Cluster-name fan-out tests (ClientFactory listener wiring) --------

  /**
   * {@link ClientConfig} owns a {@code Map<RequestType, FastClientStats>} (one entry per
   * {@link RequestType}). When the fast-client cluster-change listener fires, it needs a single
   * fan-out hook that propagates the new cluster name to every entry. This test asserts:
   * <ol>
   *   <li>The stats map covers every {@link RequestType} value (sanity — proves the fan-out's
   *       iteration is exhaustive).
   *   <li>Calling {@code onClusterNameUpdated} replaces the cluster on at least two distinct
   *       RequestType-keyed stats so that subsequent emissions carry the new value. Two is enough
   *       to prove the loop visits multiple entries; combined with the size check above this gives
   *       full coverage without enumerating every RequestType's metric prefix.
   * </ol>
   *
   * <p>Drives the new {@code ClientConfig#onClusterNameUpdated(String)} method that {@code
   * ClientFactory} will register as the cluster-change listener on {@code RequestBasedMetadata}.
   */
  @Test
  public void testOnClusterNameUpdatedFansOutToAllRequestTypeStats() {
    String storeName = "test_store";
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository repo = getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true, reader);

    ClientConfig clientConfig = getClientConfigWithMinimumRequiredInputs().setMetricsRepository(repo).build();

    // Sanity: every RequestType has a stats — proves the fan-out's iteration is exhaustive
    for (RequestType requestType: RequestType.values()) {
      assertNotNull(clientConfig.getStats(requestType), "Missing FastClientStats for RequestType " + requestType);
    }

    String newCluster = "venice-cluster-fanned-out";
    clientConfig.onClusterNameUpdated(newCluster);

    // Emit through two distinct RequestType-keyed stats; combined with the size check above this
    // proves the loop visits multiple entries, not just one.
    FastClientStats singleGetStats = clientConfig.getStats(SINGLE_GET);
    FastClientStats multiGetStats = clientConfig.getStats(MULTI_GET);
    singleGetStats.emitHealthyRequestMetricsNonDavinciClient(50.0, 1, 1);
    multiGetStats.emitHealthyRequestMetricsNonDavinciClient(75.0, 3, 3);

    Attributes singleGetAttrs = new OpenTelemetryDataTestUtils.OpenTelemetryAttributesBuilder().setStoreName(storeName)
        .setClusterName(newCluster)
        .setRequestType(SINGLE_GET)
        .setHttpStatus(HttpResponseStatusEnum.OK)
        .setVeniceStatusCategory(SUCCESS)
        .build();
    Attributes multiGetAttrs = new OpenTelemetryDataTestUtils.OpenTelemetryAttributesBuilder().setStoreName(storeName)
        .setClusterName(newCluster)
        .setRequestType(MULTI_GET)
        .setHttpStatus(HttpResponseStatusEnum.OK)
        .setVeniceStatusCategory(SUCCESS)
        .build();

    validateLongPointDataFromCounter(reader, 1, singleGetAttrs, "call_count", FAST_CLIENT.getMetricsPrefix());
    validateLongPointDataFromCounter(reader, 1, multiGetAttrs, "call_count", FAST_CLIENT.getMetricsPrefix());
  }

  /**
   * Idempotent fan-out — calling {@code onClusterNameUpdated} with the unchanged value should be a
   * no-op for every stats in the map. The per-stats {@code BasicClientStats#onClusterNameUpdated}
   * already short-circuits on the same cluster (covered by its own unit tests); this test just
   * confirms the fan-out doesn't introduce surprising side effects on a same-value re-fire.
   */
  @Test
  public void testOnClusterNameUpdatedIdempotentOnSameClusterAcrossAllRequestTypes() {
    String storeName = "test_store";
    String clusterName = "venice-cluster-stable";
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository repo = getVeniceMetricsRepository(FAST_CLIENT, CLIENT_METRIC_ENTITIES, true, reader);

    ClientConfig clientConfig = getClientConfigWithMinimumRequiredInputs().setMetricsRepository(repo).build();

    // First update lifts every stats from the bootstrap sentinel to the real cluster
    clientConfig.onClusterNameUpdated(clusterName);

    // Re-fire with the same value multiple times — should be a no-op for every stats in the map
    clientConfig.onClusterNameUpdated(clusterName);
    clientConfig.onClusterNameUpdated(clusterName);

    // One emission per RequestType; counters should still reflect a single emission each
    clientConfig.getStats(SINGLE_GET).emitHealthyRequestMetricsNonDavinciClient(20.0, 1, 1);
    clientConfig.getStats(MULTI_GET).emitHealthyRequestMetricsNonDavinciClient(30.0, 1, 1);

    Attributes singleGetAttrs = new OpenTelemetryDataTestUtils.OpenTelemetryAttributesBuilder().setStoreName(storeName)
        .setClusterName(clusterName)
        .setRequestType(SINGLE_GET)
        .setHttpStatus(HttpResponseStatusEnum.OK)
        .setVeniceStatusCategory(SUCCESS)
        .build();
    Attributes multiGetAttrs = new OpenTelemetryDataTestUtils.OpenTelemetryAttributesBuilder().setStoreName(storeName)
        .setClusterName(clusterName)
        .setRequestType(MULTI_GET)
        .setHttpStatus(HttpResponseStatusEnum.OK)
        .setVeniceStatusCategory(SUCCESS)
        .build();

    validateLongPointDataFromCounter(reader, 1, singleGetAttrs, "call_count", FAST_CLIENT.getMetricsPrefix());
    validateLongPointDataFromCounter(reader, 1, multiGetAttrs, "call_count", FAST_CLIENT.getMetricsPrefix());
  }

  @Test
  public void testClientConfigBuilderClonePreservesFactories() {
    SerializerFactory mockSerializerFactory = mock(SerializerFactory.class);
    DeserializerFactory mockDeserializerFactory = mock(DeserializerFactory.class);

    ClientConfig.ClientConfigBuilder originalBuilder = getClientConfigWithMinimumRequiredInputs();
    originalBuilder.setKeySerializerFactory(mockSerializerFactory);
    originalBuilder.setValueDeserializerFactory(mockDeserializerFactory);

    // Clone the builder
    ClientConfig.ClientConfigBuilder clonedBuilder = originalBuilder.clone();
    ClientConfig clonedConfig = clonedBuilder.build();

    // Verify factories are preserved in the clone
    assertTrue(clonedConfig.getKeySerializerFactory().isPresent());
    assertTrue(clonedConfig.getValueDeserializerFactory().isPresent());
    assertEquals(clonedConfig.getKeySerializerFactory().get(), mockSerializerFactory);
    assertEquals(clonedConfig.getValueDeserializerFactory().get(), mockDeserializerFactory);
  }
}
