package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.MultiKeyLongTailRetryPolicy;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MultiKeyRetryPolicyTest {
  @DataProvider
  public Object[][] keyCaps() {
    return new Object[][] { { 500, 500, false }, { 500, 501, false }, { 5000, 4999, false }, { 5000, 5000, false },
        { 5000, 5001, false }, { 500, 500, true }, { 500, 501, true }, { 5000, 4999, true }, { 5000, 5000, true },
        { 5000, 5001, true } };
  }

  @Test(dataProvider = "keyCaps")
  public void testRetryChainPreservesWholeRequestKeyCap(int cap, int count, boolean compute) throws Exception {
    MetricsRepository metrics = new MetricsRepository();
    try {
      ClientConfig config = builder(metrics).build();
      StoreMetadata metadata = mock(StoreMetadata.class);
      doReturn(true).when(metadata).isReady();
      doReturn("retry-policy-test").when(metadata).getStoreName();
      doReturn(Schema.create(Schema.Type.STRING)).when(metadata).getKeySchema();
      doReturn(cap).when(metadata).getBatchGetLimit();
      TransportClient transport = mock(TransportClient.class);
      TimeoutProcessor timer = mock(TimeoutProcessor.class);
      doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timer)
          .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MICROSECONDS));
      DispatchingAvroGenericStoreClient<String, VersionProperties> dispatcher =
          new DispatchingAvroGenericStoreClient<>(metadata, config, transport);
      try (RetriableAvroGenericStoreClient<String, VersionProperties> client = new RetriableAvroGenericStoreClient<>(
          dispatcher,
          config,
          timer,
          () -> MultiKeyLongTailRetryPolicy.parse("1-:8"))) {
        Set<String> keys = IntStream.range(0, count).mapToObj(Integer::toString).collect(Collectors.toSet());
        CompletableFuture<Optional<Exception>> completion = new CompletableFuture<>();
        StreamingCallback callback = new StreamingCallback() {
          @Override
          public void onRecordReceived(Object key, Object value) {
          }

          @Override
          public void onCompletion(Optional exception) {
            completion.complete(exception);
          }
        };
        if (compute) {
          client.compute(
              new ComputeRequestContext<>(count, false),
              mock(ComputeRequestWrapper.class),
              keys,
              VersionProperties.SCHEMA$,
              callback,
              0);
        } else {
          client.streamingBatchGet(new BatchGetRequestContext<>(count, false), keys, callback);
        }
        if (count > cap) {
          // Preserve the existing timer-gated error behavior, rather than changing error timing for cap failures.
          assertFalse(completion.isDone());
          ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);
          verify(timer).schedule(retry.capture(), eq(8000L), eq(TimeUnit.MICROSECONDS));
          retry.getValue().run();
          assertTrue(
              ExceptionUtils
                  .recursiveClassEquals(completion.get(5, TimeUnit.SECONDS).get(), VeniceKeyCountLimitException.class));
          verify(metadata, never()).routeRequest(any(), any());
        } else {
          assertFalse(completion.get(5, TimeUnit.SECONDS).isPresent());
          // The entire set, not a per-route subset, passed the dispatcher's inclusive cap guard.
          verify(metadata).routeRequest(any(), any());
        }
        // This fixture deliberately provides no routes. Rejected calls must never reach transport or routing.
        verifyNoInteractions(transport);
      }
    } finally {
      metrics.close();
    }
  }

  private ClientConfig.ClientConfigBuilder<String, VersionProperties, VersionProperties> builder(
      MetricsRepository metrics) {
    return new ClientConfig.ClientConfigBuilder<String, VersionProperties, VersionProperties>()
        .setStoreName("retry-policy-test")
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("discovery")
        .setMetricsRepository(metrics)
        .setRetryBudgetEnabled(false);
  }

  @DataProvider
  public Object[][] clientTypes() {
    return new Object[][] { { false, false }, { false, true }, { true, false }, { true, true } };
  }

  @Test(dataProvider = "clientTypes")
  public void testFactoryUsesLiveProviderAndPreservesEnableFlags(boolean specific, boolean compute) throws Exception {
    MetricsRepository metrics = new MetricsRepository();
    try {
      // Compute's enable flag remains false by default. The existing factory installs a retry layer when ANY
      // retry flag is true, and that layer retries both multi-key APIs. Threshold delivery must not change this.
      ClientConfig config = builder(metrics).setSpecificValueClass(VersionProperties.class).build();
      assertFalse(config.isLongTailRetryEnabledForCompute());
      StoreMetadata metadata = mock(StoreMetadata.class);
      doReturn(true).when(metadata).isReady();
      doReturn("retry-policy-test").when(metadata).getStoreName();
      doReturn(Schema.create(Schema.Type.STRING)).when(metadata).getKeySchema();
      doReturn(500).when(metadata).getBatchGetLimit();
      AtomicReference<MultiKeyLongTailRetryPolicy> policy =
          new AtomicReference<>(MultiKeyLongTailRetryPolicy.parse("1-:19"));
      doAnswer(ignored -> policy.get()).when(metadata).getMultiKeyLongTailRetryPolicy();
      TimeoutProcessor timer = mock(TimeoutProcessor.class);
      doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timer)
          .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MICROSECONDS));
      InstanceHealthMonitor healthMonitor = mock(InstanceHealthMonitor.class);
      doReturn(timer).when(healthMonitor).getTimeoutProcessor();
      doReturn(healthMonitor).when(metadata).getInstanceHealthMonitor();
      try (AvroGenericStoreClient<String, VersionProperties> client = specific
          ? ClientFactory.getAndStartSpecificStoreClient(metadata, config)
          : ClientFactory.getAndStartGenericStoreClient(metadata, config)) {
        for (int i = 0; i < 2; i++) {
          if (compute) {
            ((InternalAvroStoreClient<String, VersionProperties>) client).compute(
                mock(ComputeRequestWrapper.class),
                Collections.singleton("key"),
                VersionProperties.SCHEMA$,
                mock(StreamingCallback.class),
                0);
          } else {
            client.streamingBatchGet(Collections.singleton("key"), mock(StreamingCallback.class));
          }
          policy.set(MultiKeyLongTailRetryPolicy.parse("1-:29"));
        }
        verify(timer).schedule(any(Runnable.class), eq(19000L), eq(TimeUnit.MICROSECONDS));
        verify(timer).schedule(any(Runnable.class), eq(29000L), eq(TimeUnit.MICROSECONDS));
      }
    } finally {
      metrics.close();
    }
  }

  @Test(dataProvider = "clientTypes")
  public void testLivePolicySchedulesEachRequestOnce(boolean specific, boolean compute) {
    MetricsRepository metrics = new MetricsRepository();
    try {
      ClientConfig config = builder(metrics).build();
      AtomicReference<MultiKeyLongTailRetryPolicy> policy =
          new AtomicReference<>(MultiKeyLongTailRetryPolicy.parse("1-500:9,501-5000:17,5001-:23"));
      TimeoutProcessor timer = mock(TimeoutProcessor.class);
      doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timer)
          .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MICROSECONDS));
      InternalAvroStoreClient<String, VersionProperties> delegate = mock(InternalAvroStoreClient.class);
      try (RetriableAvroGenericStoreClient<String, VersionProperties> client = specific
          ? new RetriableAvroSpecificStoreClient<>(delegate, config, timer, policy::get)
          : new RetriableAvroGenericStoreClient<>(delegate, config, timer, policy::get)) {
        int[] keyCounts = { 500, 501, 4999, 5000, 5001 };
        for (int count: keyCounts) {
          request(client, compute, count);
        }
        ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);
        verify(timer, times(5)).schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.MICROSECONDS));
        assertEquals(delays.getAllValues(), Arrays.asList(9000L, 17000L, 17000L, 17000L, 23000L));
        policy.set(MultiKeyLongTailRetryPolicy.parse("1-:31"));
        // Publication alone must not retime any pending request.
        verify(timer, times(5)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MICROSECONDS));
        request(client, compute, 500);
        verify(timer).schedule(any(Runnable.class), eq(31000L), eq(TimeUnit.MICROSECONDS));
        policy.set(null);
        request(client, compute, 500);
        verify(timer).schedule(any(Runnable.class), eq(100000L), eq(TimeUnit.MICROSECONDS));
      }
    } finally {
      metrics.close();
    }
  }

  @DataProvider
  public Object[][] precedence() {
    return new Object[][] {
        // fixed, batch range, compute range, expected batch us, expected compute us
        { 0, null, null, 31000, 31000 }, { 123, "1-:19", "1-:29", 123, 29000 }, { 0, "1-:19", null, 19000, 31000 },
        { 0, null, "1-:29", 31000, 29000 },
        { 0, ClientConfig.LONG_TAIL_RANGE_BASED_RETRY_THRESHOLD_FOR_BATCH_GET_IN_MILLI_SECONDS,
            ClientConfig.LONG_TAIL_RANGE_BASED_RETRY_THRESHOLD_FOR_COMPUTE_IN_MILLI_SECONDS, 8000, 8000 } };
  }

  @Test(dataProvider = "precedence")
  public void testPrecedenceAndClone(int fixed, String batch, String compute, int batchUs, int computeUs) {
    MetricsRepository metrics = new MetricsRepository();
    try {
      ClientConfig.ClientConfigBuilder<String, VersionProperties, VersionProperties> builder = builder(metrics);
      builder.setLongTailRetryThresholdForBatchGetInMicroSeconds(fixed);
      if (batch != null) {
        builder.setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds(batch);
      }
      if (compute != null) {
        builder.setLongTailRangeBasedRetryThresholdForComputeInMilliSeconds(compute);
      }
      ClientConfig config = builder.clone().clone().build();
      assertEquals(config.isBatchGetRetryRangeExplicit(), batch != null);
      assertEquals(config.isComputeRetryRangeExplicit(), compute != null);
      TimeoutProcessor timer = mock(TimeoutProcessor.class);
      doReturn(mock(TimeoutProcessor.TimeoutFuture.class)).when(timer)
          .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MICROSECONDS));
      try (RetriableAvroGenericStoreClient<String, VersionProperties> client = new RetriableAvroGenericStoreClient<>(
          mock(InternalAvroStoreClient.class),
          config,
          timer,
          () -> MultiKeyLongTailRetryPolicy.parse("1-:31"))) {
        request(client, false, 1);
        request(client, true, 1);
        ArgumentCaptor<Long> delays = ArgumentCaptor.forClass(Long.class);
        verify(timer, times(2)).schedule(any(Runnable.class), delays.capture(), eq(TimeUnit.MICROSECONDS));
        assertEquals(delays.getAllValues(), Arrays.asList((long) batchUs, (long) computeUs));
      }
    } finally {
      metrics.close();
    }
  }

  @Test
  public void testCloneDoesNotMarkDefaultsExplicit() {
    MetricsRepository metrics = new MetricsRepository();
    try {
      ClientConfig.ClientConfigBuilder<String, VersionProperties, VersionProperties> original = builder(metrics);
      ClientConfig implicit = original.clone().build();
      assertFalse(implicit.isBatchGetRetryRangeExplicit());
      assertFalse(implicit.isComputeRetryRangeExplicit());
      original.setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds(
          implicit.getLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds());
      original.setLongTailRangeBasedRetryThresholdForComputeInMilliSeconds(
          implicit.getLongTailRangeBasedRetryThresholdForComputeInMilliSeconds());
      ClientConfig explicit = original.clone().build();
      assertTrue(explicit.isBatchGetRetryRangeExplicit());
      assertTrue(explicit.isComputeRetryRangeExplicit());
    } finally {
      metrics.close();
    }
  }

  private void request(RetriableAvroGenericStoreClient<String, VersionProperties> client, boolean compute, int count) {
    Set<String> keys = count == 0
        ? Collections.emptySet()
        : IntStream.range(0, count).mapToObj(Integer::toString).collect(Collectors.toSet());
    if (compute) {
      client.compute(new ComputeRequestContext<>(count, false), null, keys, null, mock(StreamingCallback.class), 0);
    } else {
      client.streamingBatchGet(new BatchGetRequestContext<>(count, false), keys, mock(StreamingCallback.class));
    }
  }
}
