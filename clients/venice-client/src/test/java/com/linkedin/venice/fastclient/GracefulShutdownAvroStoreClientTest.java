package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.listeners.StoreConfigChangeListener;
import com.linkedin.venice.client.store.listeners.StoreVersionSwitchListener;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.meta.InstanceHealthMonitor;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class GracefulShutdownAvroStoreClientTest {
  private static final Schema RESULT_SCHEMA = Schema.create(Schema.Type.STRING);

  private static class Fixture implements AutoCloseable {
    final MetricsRepository metrics = new MetricsRepository();
    final ClientConfig config;
    final ClientRequestTracker tracker;
    final InternalAvroStoreClient<String, String> dispatch = mock(InternalAvroStoreClient.class);
    final InternalAvroStoreClient<String, String> trackedDispatch;
    final ExecutorService closeExecutor = Executors.newCachedThreadPool();
    final List<CompletableFuture<Void>> closeTasks = new ArrayList<>();
    final List<Runnable> releases = new ArrayList<>();
    GracefulShutdownAvroGenericStoreClient<String, String> client;

    Fixture(long timeout) {
      config = new ClientConfig.ClientConfigBuilder<>().setStoreName("shutdown-test")
          .setR2Client(mock(Client.class))
          .setD2Client(mock(D2Client.class))
          .setClusterDiscoveryD2Service("discovery")
          .setMetricsRepository(metrics)
          .setClientCloseTimeoutMs(timeout)
          .build();
      tracker = new ClientRequestTracker(timeout);
      trackedDispatch = new GracefulShutdownAvroGenericStoreClient<>(dispatch, config, tracker, false);
      wrap(trackedDispatch);
    }

    void wrap(InternalAvroStoreClient<String, String> delegate) {
      client = new GracefulShutdownAvroGenericStoreClient<>(delegate, config, tracker, true);
    }

    CompletableFuture<Void> closeAsync() {
      CompletableFuture<Void> task = CompletableFuture.runAsync(client::close, closeExecutor);
      closeTasks.add(task);
      return task;
    }

    CompletableFuture<Void> startClose(Runnable release) {
      CompletableFuture<Void> task = closeAsync();
      releases.add(() -> {
        if (!task.isDone()) {
          release.run();
        }
      });
      TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
        assertThrows(VeniceClientException.class, () -> {
          try (ClientRequestTracker.Ticket ignored = tracker.acquire(true, null)) {
            // Release probes admitted before the close thread starts.
          }
        });
      });
      return task;
    }

    @Override
    public void close() {
      try {
        releases.forEach(Runnable::run);
        client.close();
        CompletableFuture.allOf(closeTasks.toArray(new CompletableFuture[0])).join();
      } finally {
        closeExecutor.shutdown();
        try {
          assertTrue(closeExecutor.awaitTermination(5, TimeUnit.SECONDS), "Close threads must finish");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError(e);
        } finally {
          metrics.close();
        }
      }
    }
  }

  @Test(timeOut = 10000)
  public void testDrainRejectsNewRequestsAndKeepsCancelledAttemptAlive() {
    try (Fixture f = new Fixture(5000)) {
      CompletableFuture<String> response = new CompletableFuture<>();
      when(f.dispatch.get(any(GetRequestContext.class), eq("key"))).thenReturn(response);
      CompletableFuture<String> caller = f.client.get("key");
      assertTrue(caller.cancel(false));
      f.startClose(() -> response.complete("value"));
      assertThrows(VeniceClientException.class, () -> f.client.get("new"));
      verify(f.dispatch, never()).close();
      response.complete("value");
      f.client.close();
      verify(f.dispatch, times(1)).close();
      assertTrue(caller.isCancelled());
    }
  }

  @Test(timeOut = 10000)
  public void testSingleGetDeadlineFailsCallerAndIgnoresLateResponse() {
    for (long timeout: new long[] { 0, 1 }) {
      try (Fixture f = new Fixture(timeout)) {
        CompletableFuture<String> response = new CompletableFuture<>();
        when(f.dispatch.get(any(GetRequestContext.class), any())).thenReturn(response);
        CompletableFuture<String> caller = f.client.get("key");
        f.client.close();
        assertShutdownFailure(caller, "shutdown timeout");
        response.complete("late");
        assertShutdownFailure(caller, "shutdown timeout");
        verify(f.dispatch, times(1)).close();
      }
    }
  }

  @Test(timeOut = 10000)
  public void testCloseWaitsForResourceCleanup() throws Exception {
    try (Fixture f = new Fixture(5000)) {
      CountDownLatch cleanupEntered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      doAnswer(ignored -> {
        cleanupEntered.countDown();
        assertTrue(release.await(5, TimeUnit.SECONDS));
        return null;
      }).when(f.dispatch).close();
      CompletableFuture<Void> close = f.startClose(release::countDown);
      try {
        assertTrue(cleanupEntered.await(5, TimeUnit.SECONDS));
        assertFalse(close.isDone(), "Close must wait for resource cleanup");
      } finally {
        release.countDown();
      }
      close.get(5, TimeUnit.SECONDS);
      f.client.close();
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(timeOut = 10000)
  public void testSendDoesNotHoldAdmissionLockAndConcurrentClosesCleanUpOnce() throws Exception {
    try (Fixture f = new Fixture(5000)) {
      CountDownLatch sending = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      when(f.dispatch.get(any(GetRequestContext.class), any())).thenAnswer(ignored -> {
        sending.countDown();
        assertTrue(release.await(5, TimeUnit.SECONDS));
        return CompletableFuture.completedFuture("value");
      });
      CompletableFuture<CompletableFuture<String>> send = CompletableFuture.supplyAsync(() -> f.client.get("key"));
      CompletableFuture<Void> close1;
      CompletableFuture<Void> close2;
      try {
        assertTrue(sending.await(5, TimeUnit.SECONDS));
        f.startClose(release::countDown);
        close1 = f.closeAsync();
        close2 = f.closeAsync();
        assertThrows(VeniceClientException.class, () -> f.client.get("new"));
        verify(f.dispatch, never()).close();
      } finally {
        release.countDown();
        assertEquals(send.get(5, TimeUnit.SECONDS).join(), "value");
      }
      CompletableFuture.allOf(close1, close2).get(5, TimeUnit.SECONDS);
      verify(f.dispatch, times(1)).close();
    }
  }

  @DataProvider
  public Object[][] streamingApis() {
    return new Object[][] { { "batch" }, { "streamFuture" }, { "streamCallback" }, { "compute" } };
  }

  @Test(dataProvider = "streamingApis", timeOut = 10000)
  public void testStreamingApisDrainAndIgnoreDuplicateCompletion(String api) {
    try (Fixture f = new Fixture(5000)) {
      AtomicReference<StreamingCallback> downstream = new AtomicReference<>();
      captureStreaming(f, downstream);
      CompletableFuture<?> caller = sendStreaming(f, api);
      f.startClose(() -> downstream.get().onCompletion(Optional.empty()));
      verify(f.dispatch, never()).close();
      StreamingCallback callback = downstream.get();
      callback.onRecordReceived("key", "compute".equals(api) ? mock(ComputeGenericRecord.class) : "value");
      callback.onCompletion(Optional.empty());
      callback.onCompletion(Optional.of(new VeniceClientException("duplicate")));
      assertFalse(caller.isCompletedExceptionally());
      caller.join();
      f.client.close();
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(dataProvider = "streamingApis", timeOut = 10000)
  public void testShutdownDeadlineFailsEveryStreamingApiAndSuppressesLateRecords(String api) {
    try (Fixture f = new Fixture(0)) {
      AtomicReference<StreamingCallback> downstream = new AtomicReference<>();
      captureStreaming(f, downstream);
      CompletableFuture<?> caller = sendStreaming(f, api);
      f.client.close();
      assertShutdownFailure(caller, "shutdown timeout");
      downstream.get().onRecordReceived("key", "late");
      downstream.get().onCompletion(Optional.empty());
      assertShutdownFailure(caller, "shutdown timeout");
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(timeOut = 10000)
  public void testInterruptedCloseFailsCallerAndRestoresInterrupt() {
    try (Fixture f = new Fixture(5000)) {
      when(f.dispatch.get(any(GetRequestContext.class), any())).thenReturn(new CompletableFuture<>());
      CompletableFuture<String> caller = f.client.get("key");
      Thread.currentThread().interrupt();
      try {
        f.client.close();
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        Thread.interrupted();
      }
      assertShutdownFailure(caller, "shutdown interrupted");
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(timeOut = 10000)
  public void testSynchronousSendFailureDoesNotLeakTicket() {
    try (Fixture f = new Fixture(5000)) {
      when(f.dispatch.get(any(GetRequestContext.class), any())).thenThrow(new VeniceClientException("send failed"));
      assertShutdownFailure(f.client.get("key"), "send failed");
      doThrow(new VeniceClientException("stream failed")).when(f.dispatch)
          .streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
      assertShutdownFailure(f.client.streamingBatchGet(Collections.singleton("key")), "stream failed");
      AssertionError failure = new AssertionError("fatal stream failure");
      doThrow(failure).when(f.dispatch).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
      AtomicInteger completions = new AtomicInteger();
      StreamingCallback<String, String> callback = new StreamingCallback<String, String>() {
        @Override
        public void onRecordReceived(String key, String value) {
          fail("No records expected");
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          completions.incrementAndGet();
        }
      };
      assertSame(
          expectThrows(AssertionError.class, () -> f.client.streamingBatchGet(Collections.singleton("key"), callback)),
          failure);
      f.client.close();
      assertEquals(completions.get(), 0, "A synchronous fatal send failure must not leave a shutdown callback behind");
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(timeOut = 10000)
  public void testRetryWinnerStillDrainsOriginalAndAcceptedErrorRetryIsAllowed() {
    for (boolean errorRetry: new boolean[] { false, true }) {
      try (Fixture f = new Fixture(5000)) {
        CompletableFuture<String> original = new CompletableFuture<>();
        CompletableFuture<String> retry = new CompletableFuture<>();
        when(f.dispatch.get(any(GetRequestContext.class), any())).thenReturn(original, retry);
        TimeoutProcessor timer = mock(TimeoutProcessor.class);
        AtomicReference<Runnable> retryTask = new AtomicReference<>();
        when(timer.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(call -> {
          retryTask.set(call.getArgument(0));
          return mock(TimeoutProcessor.TimeoutFuture.class);
        });
        RetriableAvroGenericStoreClient<String, String> retriable =
            new RetriableAvroGenericStoreClient<>(f.trackedDispatch, f.config, timer);
        retriable.setSingleKeyLongTailRetryManager(null);
        f.wrap(retriable);
        CompletableFuture<String> caller = f.client.get("key");
        f.startClose(() -> {
          original.complete("loser");
          retry.complete("winner");
        });
        if (errorRetry) {
          original.completeExceptionally(new VeniceClientException("retryable"));
        } else {
          retryTask.get().run();
        }
        verify(f.dispatch, times(2)).get(any(GetRequestContext.class), eq("key"));
        retry.complete("winner");
        assertEquals(caller.join(), "winner");
        if (!errorRetry) {
          verify(f.dispatch, never()).close();
          original.complete("loser");
        }
        f.client.close();
        verify(f.dispatch, times(1)).close();
        retryTask.get().run();
        verify(f.dispatch, times(2)).get(any(GetRequestContext.class), eq("key"));
      }
    }
  }

  @Test(timeOut = 10000)
  public void testDeadlineDoesNotWaitForRunningResources() throws Exception {
    try (Fixture f = new Fixture(0)) {
      CompletableFuture<String> response = new CompletableFuture<>();
      when(f.dispatch.get(any(GetRequestContext.class), any())).thenReturn(response);
      CompletableFuture<String> caller = f.client.get("key");
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch resourceEntered = new CountDownLatch(1);
      CompletableFuture<Void> work = CompletableFuture.runAsync(f.tracker.resourceTask(() -> {
        resourceEntered.countDown();
        try {
          assertTrue(release.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
      }));
      try {
        assertTrue(resourceEntered.await(5, TimeUnit.SECONDS));
        f.client.close();
        verify(f.dispatch, times(1)).close();
        assertShutdownFailure(caller, "shutdown timeout");
        assertFalse(work.isDone());
      } finally {
        release.countDown();
        work.get(5, TimeUnit.SECONDS);
      }
    }
  }

  @Test(timeOut = 10000)
  public void testThrowingShutdownCallbackDoesNotSkipCleanup() {
    try (Fixture f = new Fixture(0)) {
      f.client.streamingBatchGet(Collections.singleton("key"), new StreamingCallback<String, String>() {
        @Override
        public void onRecordReceived(String key, String value) {
          fail("No records expected");
        }

        @Override
        public void onCompletion(Optional<Exception> exception) {
          assertTrue(exception.isPresent());
          throw new VeniceClientException("Callback failed");
        }
      });
      f.client.close();
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(timeOut = 10000)
  public void testResourceTasksDrainAndQueuedTasksAreDiscardedAfterClose() throws Exception {
    try (Fixture f = new Fixture(5000)) {
      GetRequestContext<String> context = new GetRequestContext<>();
      context.clientRequestTracker = f.tracker;
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      CompletableFuture<Void> work = CompletableFuture.runAsync(context.clientResourceTask(() -> {
        entered.countDown();
        try {
          assertTrue(release.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
      }));
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        f.startClose(release::countDown);
        verify(f.dispatch, never()).close();
      } finally {
        release.countDown();
        work.get(5, TimeUnit.SECONDS);
      }
      f.client.close();
      context.clientResourceTask(() -> fail("Queued deserialization must not use closed resources")).run();
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 10000)
  public void testFactorySuppliedMetadataWrapsGenericAndSpecificClientsAndDelegatesListeners(boolean specific)
      throws Exception {
    MetricsRepository metrics = new MetricsRepository();
    ClientConfig config = new ClientConfig.ClientConfigBuilder<String, VersionProperties, VersionProperties>()
        .setStoreName("factory-shutdown-test")
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("discovery")
        .setSpecificValueClass(VersionProperties.class)
        .setMetricsRepository(metrics)
        .build();
    StoreMetadata metadata = mock(StoreMetadata.class);
    when(metadata.getStoreName()).thenReturn("factory-shutdown-test");
    InstanceHealthMonitor monitor = mock(InstanceHealthMonitor.class);
    when(metadata.getInstanceHealthMonitor()).thenReturn(monitor);
    when(monitor.getTimeoutProcessor()).thenReturn(mock(TimeoutProcessor.class));
    try (AvroGenericStoreClient<?, ?> client = specific
        ? ClientFactory.getAndStartSpecificStoreClient(metadata, config)
        : ClientFactory.getAndStartGenericStoreClient(metadata, config)) {
      assertTrue(client instanceof GracefulShutdownAvroGenericStoreClient);
      assertEquals(client instanceof AvroSpecificStoreClient, specific);
      verify(metadata).start();
      StoreVersionSwitchListener versionListener = mock(StoreVersionSwitchListener.class);
      StoreConfigChangeListener configListener = mock(StoreConfigChangeListener.class);
      client.registerVersionSwitchListener(versionListener);
      client.registerStoreConfigChangeListener(configListener);
      verify(metadata).registerVersionSwitchListener(versionListener);
      verify(metadata).registerStoreConfigChangeListener(configListener);
      client.close();
      verify(metadata, times(1)).close();
    } finally {
      metrics.close();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class, timeOut = 10000)
  public void testMultiKeyRetryWinnerDrainsBothCallbacks(boolean compute) {
    try (Fixture f = new Fixture(5000)) {
      List<StreamingCallback> attempts = new ArrayList<>();
      doAnswer(call -> {
        attempts.add(call.getArgument(2));
        return null;
      }).when(f.dispatch).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
      doAnswer(call -> {
        attempts.add(call.getArgument(4));
        return null;
      }).when(f.dispatch).compute(any(ComputeRequestContext.class), any(), any(), any(), any(), anyLong());
      TimeoutProcessor timer = mock(TimeoutProcessor.class);
      AtomicReference<Runnable> retryTask = new AtomicReference<>();
      when(timer.schedule(any(Runnable.class), anyLong(), any())).thenAnswer(call -> {
        retryTask.set(call.getArgument(0));
        return mock(TimeoutProcessor.TimeoutFuture.class);
      });
      f.wrap(new RetriableAvroGenericStoreClient<>(f.trackedDispatch, f.config, timer));
      CompletableFuture<?> caller = sendStreaming(f, compute ? "compute" : "streamFuture");
      retryTask.get().run();
      assertEquals(attempts.size(), 2);
      attempts.get(1).onRecordReceived("key", compute ? mock(ComputeGenericRecord.class) : "winner");
      caller.join();
      f.startClose(() -> attempts.forEach(attempt -> attempt.onCompletion(Optional.empty())));
      verify(f.dispatch, never()).close();
      attempts.get(0).onCompletion(Optional.empty());
      verify(f.dispatch, never()).close();
      attempts.get(1).onCompletion(Optional.empty());
      f.client.close();
      verify(f.dispatch, times(1)).close();
    }
  }

  @Test(timeOut = 10000)
  public void testDualReadThinWinnerStillDrainsFastClientAndBatchDelegates() {
    try (Fixture f = new Fixture(5000)) {
      AvroGenericStoreClient<String, String> thin = mock(AvroGenericStoreClient.class);
      CompletableFuture<String> fast = new CompletableFuture<>();
      AtomicReference<StreamingCallback<String, String>> fastBatch = new AtomicReference<>();
      when(f.dispatch.get(any(GetRequestContext.class), any())).thenReturn(fast);
      doAnswer(call -> {
        fastBatch.set(call.getArgument(2));
        return null;
      }).when(f.dispatch).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
      when(thin.get("key")).thenReturn(CompletableFuture.completedFuture("thin"));
      when(thin.batchGet(any())).thenReturn(CompletableFuture.completedFuture(Collections.singletonMap("key", "thin")));
      f.wrap(new DualReadAvroGenericStoreClient<>(f.trackedDispatch, f.config, thin));
      assertEquals(f.client.get("key").join(), "thin");
      assertEquals(f.client.batchGet(Collections.singleton("key")).join(), Collections.singletonMap("key", "thin"));
      f.startClose(() -> {
        fast.complete("fast");
        fastBatch.get().onCompletion(Optional.empty());
      });
      verify(f.dispatch, never()).close();
      fast.complete("fast");
      verify(f.dispatch, never()).close();
      fastBatch.get().onRecordReceived("key", "fast");
      fastBatch.get().onCompletion(Optional.empty());
      f.client.close();
      verify(f.dispatch, times(1)).close();
    }
  }

  private void captureStreaming(Fixture f, AtomicReference<StreamingCallback> callback) {
    doAnswer(call -> {
      callback.set(call.getArgument(2));
      return null;
    }).when(f.dispatch).streamingBatchGet(any(BatchGetRequestContext.class), any(), any());
    doAnswer(call -> {
      callback.set(call.getArgument(4));
      return null;
    }).when(f.dispatch).compute(any(ComputeRequestContext.class), any(), any(), any(), any(), anyLong());
    // A mocked internal client does not supply the base class's future-to-callback adapter.
    when(f.dispatch.batchGet(any(BatchGetRequestContext.class), any())).thenCallRealMethod();
    when(f.dispatch.streamingBatchGet(any(BatchGetRequestContext.class), any())).thenCallRealMethod();
  }

  private CompletableFuture<?> sendStreaming(Fixture f, String api) {
    if ("batch".equals(api)) {
      return f.client.batchGet(Collections.singleton("key"));
    }
    if ("streamFuture".equals(api)) {
      return f.client.streamingBatchGet(Collections.singleton("key"));
    }
    CompletableFuture<Void> completed = new CompletableFuture<>();
    AtomicInteger completions = new AtomicInteger();
    StreamingCallback callback = new StreamingCallback() {
      @Override
      public void onRecordReceived(Object key, Object value) {
        assertFalse(completed.isDone(), "No records may arrive after completion");
      }

      @Override
      public void onCompletion(Optional exception) {
        assertEquals(completions.incrementAndGet(), 1);
        if (exception.isPresent()) {
          completed.completeExceptionally((Exception) exception.get());
        } else {
          completed.complete(null);
        }
      }
    };
    if ("compute".equals(api)) {
      f.client.compute(mock(ComputeRequestWrapper.class), Collections.singleton("key"), RESULT_SCHEMA, callback, 0);
    } else {
      f.client.streamingBatchGet(Collections.singleton("key"), callback);
    }
    return completed;
  }

  private void assertShutdownFailure(CompletableFuture<?> future, String message) {
    assertTrue(future.isDone(), "Shutdown must finish pending callers before close returns");
    try {
      future.join();
      fail("Expected failure");
    } catch (java.util.concurrent.CompletionException e) {
      assertTrue(e.getCause() instanceof VeniceClientException);
      assertTrue(e.getCause().getMessage().contains(message), e.getCause().toString());
    }
  }
}
