package com.linkedin.venice.fastclient;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.read.RequestType.MULTI_GET;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.ClientType.FAST_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.fastclient.stats.FastClientStats;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Optional;
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
