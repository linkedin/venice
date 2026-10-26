package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.avro.Schema;


/**
 * The outer tier admits application requests; the dispatch tier tracks each accepted transport attempt.
 * Both share a tracker, so an early retry/dual-read winner cannot close resources used by a slower attempt.
 */
public class GracefulShutdownAvroGenericStoreClient<K, V> extends DelegatingAvroStoreClient<K, V> {
  private final ClientRequestTracker tracker;
  private final boolean applicationRequests;
  private final InternalAvroStoreClient<K, V> requestDelegate;

  public GracefulShutdownAvroGenericStoreClient(
      InternalAvroStoreClient<K, V> delegate,
      ClientConfig config,
      ClientRequestTracker tracker,
      boolean applicationRequests) {
    super(delegate, config);
    this.tracker = tracker;
    this.applicationRequests = applicationRequests;
    this.requestDelegate = delegate;
  }

  ClientRequestTracker getRequestTracker() {
    return tracker;
  }

  private <T> CompletableFuture<T> trackFuture(RequestContext context, Supplier<CompletableFuture<T>> send) {
    CompletableFuture<T> result = new CompletableFuture<>();
    ClientRequestTracker.Ticket ticket =
        tracker.acquire(applicationRequests, applicationRequests ? result::completeExceptionally : null);
    context.clientRequestTracker = tracker;
    ticket.retain();
    try {
      send.get().whenComplete((value, error) -> {
        try {
          tracker.invokeCallback(() -> {
            if (tracker.isStopped()) {
              return;
            } else if (error == null) {
              result.complete(value);
            } else {
              result.completeExceptionally(error);
            }
          });
        } finally {
          ticket.close();
        }
      });
    } catch (Throwable e) {
      try {
        tracker.invokeCallback(() -> {
          if (!tracker.isStopped()) {
            result.completeExceptionally(e);
          }
        });
      } finally {
        ticket.close();
      }
    } finally {
      ticket.close();
    }
    return result;
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext<K> context, K key) {
    return trackFuture(context, () -> super.get(context, key));
  }

  @Override
  protected CompletableFuture<Map<K, V>> batchGet(BatchGetRequestContext<K, V> context, Set<K> keys) {
    return trackFuture(context, () -> requestDelegate.batchGet(context, keys));
  }

  private <T> void trackStream(
      RequestContext context,
      StreamingCallback<K, T> callback,
      java.util.function.Consumer<StreamingCallback<K, T>> send) {
    AtomicBoolean completed = new AtomicBoolean();
    StreamingCallback<K, T> guarded = new StreamingCallback<K, T>() {
      @Override
      public synchronized void onRecordReceived(K key, T value) {
        if (!completed.get() && !tracker.isStopped()) {
          tracker.invokeCallback(() -> callback.onRecordReceived(key, value));
        }
      }

      @Override
      public synchronized void onCompletion(Optional<Exception> exception) {
        if (completed.compareAndSet(false, true)) {
          tracker.invokeCallback(() -> callback.onCompletion(exception));
        }
      }
    };
    ClientRequestTracker.Ticket ticket = tracker
        .acquire(applicationRequests, applicationRequests ? error -> guarded.onCompletion(Optional.of(error)) : null);
    context.clientRequestTracker = tracker;
    ticket.retain();
    AtomicBoolean released = new AtomicBoolean();
    StreamingCallback<K, T> tracked = new StreamingCallback<K, T>() {
      @Override
      public void onRecordReceived(K key, T value) {
        guarded.onRecordReceived(key, value);
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        try {
          if (!tracker.isStopped()) {
            guarded.onCompletion(exception);
          }
        } finally {
          if (released.compareAndSet(false, true)) {
            ticket.close();
          }
        }
      }
    };
    try {
      send.accept(tracked);
    } catch (Exception e) {
      tracked.onCompletion(Optional.of(e));
    } catch (Error e) {
      if (released.compareAndSet(false, true)) {
        ticket.close();
      }
      throw e;
    } finally {
      ticket.close();
    }
  }

  @Override
  protected void streamingBatchGet(
      BatchGetRequestContext<K, V> context,
      Set<K> keys,
      StreamingCallback<K, V> callback) {
    trackStream(context, callback, tracked -> super.streamingBatchGet(context, keys, tracked));
  }

  @Override
  protected void compute(
      ComputeRequestContext<K, V> context,
      ComputeRequestWrapper request,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) {
    trackStream(
        context,
        callback,
        tracked -> super.compute(context, request, keys, resultSchema, tracked, preRequestTimeInNS));
  }

  @Override
  public void close() {
    if (applicationRequests) {
      tracker.close(super::close);
    } else {
      super.close();
    }
  }
}
