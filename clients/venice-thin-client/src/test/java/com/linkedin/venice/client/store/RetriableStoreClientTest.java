package com.linkedin.venice.client.store;

import static com.linkedin.venice.client.stats.BasicClientStats.CLIENT_METRIC_ENTITIES;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.ClientType.THIN_CLIENT;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class RetriableStoreClientTest {
  private static final String CLUSTER_NAME = "venice-cluster";

  private StatTrackingStoreClient<String, Object> mockStoreClient;

  @BeforeTest
  public void setUp() {
    mockStoreClient = mock(StatTrackingStoreClient.class);

    String storeName = Utils.getUniqueString("store");
    doReturn(storeName).when(mockStoreClient).getStoreName();
  }

  @Test
  public void testThrowExceptionOnSameExecutors() {
    ClientConfig cc = ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName());
    ExecutorService testExecutor = Executors.newFixedThreadPool(1);
    cc.setRetryExecutor(testExecutor);
    cc.setDeserializationExecutor(testExecutor);
    try {
      assertThrows(VeniceClientException.class, () -> new RetriableStoreClient<>(mockStoreClient, cc));
    } finally {
      testExecutor.shutdownNow();
    }
  }

  @Test
  public void testGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.complete(mockReturnObject);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;
    });

    doReturn(mockInnerFuture).when(mockStoreClient).get(any(), any(), anyLong());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(mockReturnObject, retriableStoreClient.get("key", Optional.empty(), 1234).get());
  }

  @Test
  public void testGetRetry() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object> mockInnerFuture1 = new CompletableFuture();
    mockInnerFuture1.complete(mockReturnObject);

    mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    mockInnerFuture1.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });

    doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).get(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(mockReturnObject, retriableStoreClient.get("key").get());
  }

  @Test
  public void testGetRetryWithBackOffAndCount() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Object mockReturnObject = mock(Object.class);
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object> mockInnerFuture1 = new CompletableFuture();
    mockInnerFuture1.complete(mockReturnObject);

    mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    mockInnerFuture1.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });

    // Verify retry count and retry backoff can be configured
    doReturn(mockInnerFuture).doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).get(any());
    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName())
            .setRetryCount(3)
            .setRetryBackOffInMs(100));
    Assert.assertEquals(mockReturnObject, retriableStoreClient.get("key").get());
  }

  @Test
  public void testMultiGet() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Map<String, String> result = new HashMap<>();
    Set<String> keySet = new HashSet<>();
    String keyPrefix = "key_";
    for (int i = 0; i < 5; ++i) {
      result.put(keyPrefix + i, "value_" + i);
    }
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    mockInnerFuture.complete(result);
    mockInnerFuture = mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;

    });

    doReturn(mockInnerFuture).when(mockStoreClient).batchGet(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(result, retriableStoreClient.batchGet(keySet).get());
  }

  @Test
  public void testMultiGetRetry() throws ExecutionException, InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    Map<String, String> result = new HashMap<>();
    Set<String> keySet = new HashSet<>();
    String keyPrefix = "key_";
    for (int i = 0; i < 5; ++i) {
      result.put(keyPrefix + i, "value_" + i);
    }
    for (int i = 0; i < 10; ++i) {
      keySet.add(keyPrefix + i);
    }
    mockInnerFuture.completeExceptionally(new VeniceClientHttpException(503));
    CompletableFuture<Object> mockInnerFuture1 = new CompletableFuture();
    mockInnerFuture1.complete(result);

    mockInnerFuture.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    mockInnerFuture1.handle((value, throwable) -> {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return value;
    });

    doReturn(mockInnerFuture).doReturn(mockInnerFuture1).when(mockStoreClient).batchGet(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    Assert.assertEquals(result, retriableStoreClient.batchGet(keySet).get());
  }

  @Test
  public void testGetWithException() throws InterruptedException {
    CompletableFuture<Object> mockInnerFuture = new CompletableFuture();
    mockInnerFuture.completeExceptionally(
        new VeniceClientHttpException("Inner mock exception", HttpResponseStatus.BAD_REQUEST.code()));
    doReturn(mockInnerFuture).when(mockStoreClient).get(any());

    RetriableStoreClient<String, Object> retriableStoreClient = new RetriableStoreClient<>(
        mockStoreClient,
        ClientConfig.defaultGenericClientConfig(mockStoreClient.getStoreName()));
    try {
      retriableStoreClient.get("key").get();
      Assert.fail("ExecutionException should be thrown");
    } catch (ExecutionException e) {
      System.out.println(e);
      // expected
    }
  }

  @Test
  public void testGetRecordsSingleGetStatsThroughStatTrackingClient() throws Exception {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository repository = getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true, reader);
    try {
      String storeName = Utils.getUniqueString("store");
      InternalAvroStoreClient<String, Object> innerClient = mockInnerClient(storeName);
      doReturn(CompletableFuture.completedFuture(mock(Object.class))).when(innerClient).get(any(), any(), anyLong());
      RetriableStoreClient<String, Object> retriableStoreClient =
          createRetriableStatTrackingClient(innerClient, storeName, repository);

      retriableStoreClient.get("key").get();

      validateLongPointDataFromCounter(
          reader,
          1,
          expectedSingleGetSuccessAttributes(storeName),
          "call_count",
          THIN_CLIENT.getMetricsPrefix());
      Map<String, ? extends Metric> metrics = repository.metrics();
      Assert.assertTrue(metrics.get("." + storeName + "--request.OccurrenceRate").value() > 0.0);
      Assert.assertTrue(metrics.get("." + storeName + "--healthy_request.OccurrenceRate").value() > 0.0);
      Assert.assertEquals(metrics.get("." + storeName + "--success_request_key_count.Avg").value(), 1.0);
    } finally {
      repository.close();
    }
  }

  @Test
  public void testGetRetryRecordsSingleGetStatsThroughStatTrackingClient() throws Exception {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsRepository repository = getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true, reader);
    try {
      String storeName = Utils.getUniqueString("store");
      InternalAvroStoreClient<String, Object> innerClient = mockInnerClient(storeName);
      CompletableFuture<Object> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new VeniceClientHttpException(503));
      doReturn(failedFuture).doReturn(CompletableFuture.completedFuture(mock(Object.class)))
          .when(innerClient)
          .get(any(), any(), anyLong());
      RetriableStoreClient<String, Object> retriableStoreClient =
          createRetriableStatTrackingClient(innerClient, storeName, repository);

      retriableStoreClient.get("key").get();

      validateLongPointDataFromCounter(
          reader,
          1,
          expectedSingleGetSuccessAttributes(storeName),
          "call_count",
          THIN_CLIENT.getMetricsPrefix());
      Map<String, ? extends Metric> metrics = repository.metrics();
      Assert.assertTrue(metrics.get("." + storeName + "--healthy_request.OccurrenceRate").value() > 0.0);
      Assert.assertTrue(metrics.get("." + storeName + "--unhealthy_request.OccurrenceRate").value() > 0.0);
    } finally {
      repository.close();
    }
  }

  @Test
  public void testGetRawRecordsSchemaReaderStatsThroughStatTrackingClient() throws Exception {
    VeniceMetricsRepository repository = getVeniceMetricsRepository(THIN_CLIENT, CLIENT_METRIC_ENTITIES, true);
    try {
      String storeName = Utils.getUniqueString("store");
      InternalAvroStoreClient<String, Object> innerClient = mockInnerClient(storeName);
      doReturn(CompletableFuture.completedFuture(new byte[0])).when(innerClient).getRaw(any(), any(), anyLong());
      RetriableStoreClient<String, Object> retriableStoreClient =
          createRetriableStatTrackingClient(innerClient, storeName, repository);

      retriableStoreClient.getRaw("key_schema/" + storeName).get();

      Map<String, ? extends Metric> metrics = repository.metrics();
      String schemaReaderPrefix = "." + storeName + "_schema_reader";
      Assert.assertTrue(metrics.get(schemaReaderPrefix + "--request.OccurrenceRate").value() > 0.0);
      Assert.assertTrue(metrics.get(schemaReaderPrefix + "--healthy_request.OccurrenceRate").value() > 0.0);
    } finally {
      repository.close();
    }
  }

  @SuppressWarnings("unchecked")
  private static InternalAvroStoreClient<String, Object> mockInnerClient(String storeName) {
    InternalAvroStoreClient<String, Object> innerClient = mock(InternalAvroStoreClient.class);
    doReturn(storeName).when(innerClient).getStoreName();
    return innerClient;
  }

  @SuppressWarnings("unchecked")
  private static RetriableStoreClient<String, Object> createRetriableStatTrackingClient(
      InternalAvroStoreClient<String, Object> innerClient,
      String storeName,
      VeniceMetricsRepository repository) {
    ClientConfig clientConfig =
        ClientConfig.defaultGenericClientConfig(storeName).setMetricsRepository(repository).setRetryOnRouterError(true);
    StatTrackingStoreClient<String, Object> statTrackingStoreClient =
        new StatTrackingStoreClient<>(innerClient, clientConfig);
    // Pin the cluster-name dimension so the expected OTel attributes are deterministic.
    ArgumentCaptor<Consumer<String>> listenerCaptor = ArgumentCaptor.forClass(Consumer.class);
    verify(innerClient).setClusterNameChangeListener(listenerCaptor.capture());
    listenerCaptor.getValue().accept(CLUSTER_NAME);
    return new RetriableStoreClient<>(statTrackingStoreClient, clientConfig);
  }

  private static Attributes expectedSingleGetSuccessAttributes(String storeName) {
    return new OpenTelemetryDataTestUtils.OpenTelemetryAttributesBuilder().setStoreName(storeName)
        .setClusterName(CLUSTER_NAME)
        .setRequestType(SINGLE_GET)
        .setHttpStatus(HttpResponseStatusEnum.OK)
        .setVeniceStatusCategory(SUCCESS)
        .build();
  }
}
