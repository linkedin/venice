package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Focused tests for the optimization where a {@code batchGet}/{@code streamingBatchGet} containing a single key is
 * served via a single-GET request instead of a multi-get request (see
 * {@link AbstractAvroStoreClient#streamingBatchGet(java.util.Set, com.linkedin.venice.client.store.streaming.StreamingCallback)}).
 *
 * <p>The {@link TransportClient} is mocked so the routing decision is directly observable: a single-key request must
 * invoke {@link TransportClient#get} (single-GET) and never {@link TransportClient#streamPost} (multi-get), while a
 * multi-key request must do the opposite. The shared {@code SimpleStoreClient} test fixture and value schema are
 * reused from {@link AbstractAvroStoreClientTest}.
 */
public class SingleKeyBatchGetAsSingleGetTest {
  private static GenericRecord buildValue() {
    GenericRecord recordFieldValue =
        new GenericData.Record(AbstractAvroStoreClientTest.VALUE_SCHEMA.getField("record_field").schema());
    recordFieldValue.put("nested_field1", 5.1d);

    GenericRecord value = new GenericData.Record(AbstractAvroStoreClientTest.VALUE_SCHEMA);
    value.put("int_field", 1);
    value.put("float_field", 1.1f);
    value.put("record_field", recordFieldValue);
    value.put("float_array_field1", Collections.emptyList());
    value.put("float_array_field2", Collections.emptyList());
    value.put("int_array_field2", Collections.emptyList());
    return value;
  }

  private static AbstractAvroStoreClientTest.SimpleStoreClient<String, GenericRecord> newStoreClient(
      TransportClient transportClient) {
    return new AbstractAvroStoreClientTest.SimpleStoreClient<>(
        transportClient,
        "test_store",
        true,
        AbstractAvroStoreClient.getDefaultDeserializationExecutor());
  }

  /**
   * Base mock that fails the test if {@code streamPost} (multi-get) is invoked, so single-key routing is asserted.
   */
  private abstract static class SingleGetOnlyTransportClient extends TransportClient {
    final AtomicInteger getCount = new AtomicInteger();
    final AtomicInteger streamPostCount = new AtomicInteger();

    @Override
    public CompletableFuture<TransportClientResponse> post(
        String requestPath,
        Map<String, String> headers,
        byte[] requestBody) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void streamPost(
        String requestPath,
        Map<String, String> headers,
        byte[] requestBody,
        TransportClientStreamingCallback callback,
        int keyCount) {
      streamPostCount.incrementAndGet();
      Assert.fail("Single-key batch-get must not use streamPost (multi-get)");
    }

    @Override
    public void close() {
    }
  }

  /**
   * A single-key {@code batchGet} for an existing key is served via single-GET and returns the value.
   */
  @Test
  public void testSingleKeyBatchGetRoutedToSingleGet() throws Exception {
    GenericRecord expectedValue = buildValue();
    SingleGetOnlyTransportClient transportClient = new SingleGetOnlyTransportClient() {
      @Override
      public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
        getCount.incrementAndGet();
        return CompletableFuture.completedFuture(
            new TransportClientResponse(
                1,
                CompressionStrategy.NO_OP,
                AbstractAvroStoreClientTest.valueSerializer.serialize(expectedValue)));
      }
    };

    Map<String, GenericRecord> result = newStoreClient(transportClient).batchGet(Collections.singleton("key1")).get();

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get("key1"), expectedValue);
    Assert.assertEquals(transportClient.getCount.get(), 1);
    Assert.assertEquals(transportClient.streamPostCount.get(), 0);
  }

  /**
   * A single-key {@code batchGet} for a non-existing key (single-GET returns a {@code null} response) yields an empty
   * map. Exercises the non-existing-key branch of {@code streamingBatchGetForSingleKey}.
   */
  @Test
  public void testSingleKeyBatchGetMissingKey() throws Exception {
    SingleGetOnlyTransportClient transportClient = new SingleGetOnlyTransportClient() {
      @Override
      public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
        getCount.incrementAndGet();
        // A null response indicates the key does not exist.
        return CompletableFuture.completedFuture(null);
      }
    };

    Map<String, GenericRecord> result =
        newStoreClient(transportClient).batchGet(Collections.singleton("missingKey")).get();

    Assert.assertTrue(result.isEmpty());
    Assert.assertEquals(transportClient.getCount.get(), 1);
    Assert.assertEquals(transportClient.streamPostCount.get(), 0);
  }

  /**
   * A single-key {@code batchGet} propagates a single-GET failure to the caller. Exercises the error branch of
   * {@code streamingBatchGetForSingleKey} and its {@code toException} unwrapping.
   */
  @Test
  public void testSingleKeyBatchGetErrorPropagated() {
    SingleGetOnlyTransportClient transportClient = new SingleGetOnlyTransportClient() {
      @Override
      public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
        getCount.incrementAndGet();
        CompletableFuture<TransportClientResponse> failed = new CompletableFuture<>();
        failed.completeExceptionally(new VeniceClientException("Simulated single-GET failure"));
        return failed;
      }
    };

    ExecutionException thrown = Assert.expectThrows(
        ExecutionException.class,
        () -> newStoreClient(transportClient).batchGet(Collections.singleton("key1")).get());
    Assert.assertTrue(thrown.getCause() instanceof VeniceClientException, "Unexpected cause: " + thrown.getCause());
    Assert.assertEquals(transportClient.getCount.get(), 1);
    Assert.assertEquals(transportClient.streamPostCount.get(), 0);
  }

  /**
   * Contrast: a multi-key {@code batchGet} must NOT be routed through single-GET; it uses {@code streamPost}
   * (multi-get). The stream is completed with an error purely to unblock the future.
   */
  @Test
  public void testMultiKeyBatchGetDoesNotRouteToSingleGet() {
    AtomicInteger getCount = new AtomicInteger();
    AtomicInteger streamPostCount = new AtomicInteger();
    TransportClient transportClient = new TransportClient() {
      @Override
      public CompletableFuture<TransportClientResponse> get(String requestPath, Map<String, String> headers) {
        getCount.incrementAndGet();
        Assert.fail("Multi-key batch-get must not use single-GET");
        return null;
      }

      @Override
      public CompletableFuture<TransportClientResponse> post(
          String requestPath,
          Map<String, String> headers,
          byte[] requestBody) {
        return CompletableFuture.completedFuture(null);
      }

      @Override
      public void streamPost(
          String requestPath,
          Map<String, String> headers,
          byte[] requestBody,
          TransportClientStreamingCallback callback,
          int keyCount) {
        streamPostCount.incrementAndGet();
        // Unblock the caller by completing the multi-get stream with an error.
        callback.onCompletion(Optional.of(new VeniceClientException("contrast: stop multi-get")));
      }

      @Override
      public void close() {
      }
    };

    Set<String> twoKeys = new HashSet<>();
    twoKeys.add("key1");
    twoKeys.add("key2");

    Assert.expectThrows(ExecutionException.class, () -> newStoreClient(transportClient).batchGet(twoKeys).get());
    Assert.assertEquals(getCount.get(), 0);
    Assert.assertEquals(streamPostCount.get(), 1);
  }
}
