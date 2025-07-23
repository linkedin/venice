package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.GrpcTransportClient;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Integration test for server-side aggregation functionality.
 * This test verifies the complete flow from client to server and back.
 */
public class ServerSideAggregationIntegrationTest {
  private StoreMetadata mockMetadata;
  private GrpcTransportClient mockGrpcTransportClient;
  private RecordSerializer<String> mockKeySerializer;
  private ServerSideAggregationRequestBuilderImpl<String> builder;

  @BeforeMethod
  public void setUp() {
    mockMetadata = mock(StoreMetadata.class);
    mockGrpcTransportClient = mock(GrpcTransportClient.class);
    mockKeySerializer = mock(RecordSerializer.class);

    builder = new ServerSideAggregationRequestBuilderImpl<>(mockMetadata, mockGrpcTransportClient, mockKeySerializer);

    // Setup default metadata behavior
    when(mockMetadata.getStoreName()).thenReturn("test_store");
    when(mockMetadata.getCurrentStoreVersion()).thenReturn(1);

    // Setup default serializer behavior
    when(mockKeySerializer.serialize(any(String.class)))
        .thenAnswer(invocation -> ((String) invocation.getArgument(0)).getBytes());
  }

  @Test
  public void testSuccessfulCountByValue() throws Exception {
    // Setup test data
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3"));

    // Setup metadata to return replicas for all partitions (0-3)
    when(mockMetadata.getReplicas(anyInt(), eq(1))).thenReturn(Arrays.asList("server1:8080", "server2:8080"));

    // Setup gRPC transport to return successful response from each partition call
    CountByValueResponse mockResponse = CountByValueResponse.newBuilder()
        .putValueCounts("value1", 5L)
        .putValueCounts("value2", 3L)
        .putValueCounts("value3", 1L)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .setResponseRCU(1) // Each partition call returns 1 RCU
        .build();

    when(mockGrpcTransportClient.countByValue(anyString(), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    // Execute the aggregation
    builder.countByValue("testField", 10);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    // Verify the result
    AggregationResponse response = future.get();
    assertNotNull(response);
    assertFalse(response.hasError());
    // Since each partition returns 1 RCU and keys might be distributed across partitions,
    // the total RCU will be the number of partition calls made
    assertTrue(response.getKeysProcessed() >= 1);

    Map<String, Long> valueCounts = response.getValueCounts();
    // Values will be aggregated from multiple partition calls
    assertTrue(valueCounts.containsKey("value1"));
    assertTrue(valueCounts.containsKey("value2"));
    assertTrue(valueCounts.containsKey("value3"));
  }

  @Test
  public void testEmptyKeys() {
    Set<String> keys = new HashSet<>();

    builder.countByValue("testField", 10);

    try {
      builder.execute(keys);
      assert false : "Should have thrown exception for empty keys";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("Keys cannot be null or empty"));
    }
  }

  @Test
  public void testNullFieldName() {
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    try {
      builder.execute(keys);
      assert false : "Should have thrown exception for missing countByValue call";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("Must call countByValue() before execute()"));
    }
  }

  @Test
  public void testInvalidTopK() {
    try {
      builder.countByValue("testField", 0);
      assert false : "Should have thrown exception for invalid topK";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("TopK must be positive"));
    }

    try {
      builder.countByValue("testField", -1);
      assert false : "Should have thrown exception for negative topK";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("TopK must be positive"));
    }
  }

  @Test
  public void testNoAvailableReplicas() {
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    // Setup metadata to return no replicas
    when(mockMetadata.getReplicas(0, 1)).thenReturn(Arrays.asList());

    builder.countByValue("testField", 10);

    try {
      builder.execute(keys);
      assert false : "Should have thrown exception for no available replicas";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("No available replicas found"));
    }
  }

  @Test
  public void testServerError() throws Exception {
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    // Setup metadata to return replicas for any partition
    when(mockMetadata.getReplicas(anyInt(), eq(1))).thenReturn(Arrays.asList("server1:8080"));

    // Setup gRPC transport to return error response
    CountByValueResponse errorResponse = CountByValueResponse.newBuilder()
        .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
        .setErrorMessage("Server error")
        .build();

    when(mockGrpcTransportClient.countByValue(anyString(), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(errorResponse));

    builder.countByValue("testField", 10);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    try {
      future.get();
      assert false : "Should have thrown exception for server error";
    } catch (Exception e) {
      // The exception might be wrapped in ExecutionException
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      assertTrue(cause instanceof VeniceClientException);
      assertTrue(cause.getMessage().contains("Server-side aggregation failed"));
    }
  }

  @Test
  public void testServerSideAggregation() throws Exception {
    // Setup test data with multiple keys
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3", "key4"));

    // Setup metadata to return any replica (client sends to any server)
    when(mockMetadata.getReplicas(0, 1)).thenReturn(Arrays.asList("server1:8080"));

    // Setup gRPC transport to return aggregated response from server
    CountByValueResponse mockResponse = CountByValueResponse.newBuilder()
        .putValueCounts("electronics", 12L)
        .putValueCounts("books", 4L)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .setResponseRCU(4) // Total keys processed
        .build();

    when(mockGrpcTransportClient.countByValue(anyString(), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    // Execute the aggregation
    builder.countByValue("category", 5);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    // Verify that the client made only ONE call to the server (server handles all partitions)
    verify(mockGrpcTransportClient, times(1)).countByValue(anyString(), any(CountByValueRequest.class));

    // Verify the response contains the server-aggregated results
    AggregationResponse response = future.get();
    assertNotNull(response);
    assertFalse(response.hasError());

    Map<String, Long> valueCounts = response.getValueCounts();
    assertEquals(valueCounts.size(), 2);
    assertEquals(valueCounts.get("electronics"), Long.valueOf(12L));
    assertEquals(valueCounts.get("books"), Long.valueOf(4L));
  }

  @Test
  public void testSingleKeyAggregation() throws Exception {
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Setup metadata to return any replica
    when(mockMetadata.getReplicas(0, 1)).thenReturn(Arrays.asList("server1:8080"));

    CountByValueResponse mockResponse = CountByValueResponse.newBuilder()
        .putValueCounts("test", 2L)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .setResponseRCU(2)
        .build();

    when(mockGrpcTransportClient.countByValue(anyString(), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    builder.countByValue("field", 10);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    // Verify single call to server
    verify(mockGrpcTransportClient, times(1)).countByValue(anyString(), any(CountByValueRequest.class));

    AggregationResponse response = future.get();
    assertNotNull(response);
    assertFalse(response.hasError());
    assertEquals(response.getValueCounts().get("test"), Long.valueOf(2L));
  }
}
