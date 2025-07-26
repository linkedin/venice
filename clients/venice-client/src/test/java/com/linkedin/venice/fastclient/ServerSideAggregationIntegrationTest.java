package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
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
import com.linkedin.venice.protocols.ValueCount;
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

    // Setup metadata for client-side partitioning
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0, 1, 0); // key1->partition0,
                                                                                     // key2->partition1,
                                                                                     // key3->partition0
    when(mockMetadata.getReplicas(1, 0)).thenReturn(Arrays.asList("server1:8080"));
    when(mockMetadata.getReplicas(1, 1)).thenReturn(Arrays.asList("server2:8080"));

    // Setup gRPC transport to return different responses from each partition
    ValueCount partition0FieldCounts =
        ValueCount.newBuilder().putValueToCounts("value1", 3).putValueToCounts("value2", 2).build();

    ValueCount partition1FieldCounts =
        ValueCount.newBuilder().putValueToCounts("value1", 2).putValueToCounts("value3", 1).build();

    CountByValueResponse partition0Response = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("testField", partition0FieldCounts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    CountByValueResponse partition1Response = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("testField", partition1FieldCounts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    when(mockGrpcTransportClient.countByValue(eq("server1:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(partition0Response));
    when(mockGrpcTransportClient.countByValue(eq("server2:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(partition1Response));

    // Execute the aggregation
    builder.countByValue("testField", 10);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    // Verify the result
    AggregationResponse response = future.get();
    assertNotNull(response);
    assertFalse(response.hasError());

    Map<String, Integer> valueCounts = response.getValueCounts();
    // Client aggregates results from both partitions
    assertTrue(valueCounts.containsKey("value1"));
    assertTrue(valueCounts.containsKey("value2"));
    assertTrue(valueCounts.containsKey("value3"));
    assertEquals(valueCounts.get("value1"), Integer.valueOf(5)); // 3+2 from both partitions
    assertEquals(valueCounts.get("value2"), Integer.valueOf(2)); // only from partition0
    assertEquals(valueCounts.get("value3"), Integer.valueOf(1)); // only from partition1

    // Test multi-field response
    Map<String, Map<String, Integer>> fieldToValueCounts = response.getFieldToValueCounts();
    assertNotNull(fieldToValueCounts);
    assertTrue(fieldToValueCounts.containsKey("testField"));
    Map<String, Integer> testFieldCounts = fieldToValueCounts.get("testField");
    assertEquals(testFieldCounts.get("value1"), Integer.valueOf(5));
    assertEquals(testFieldCounts.get("value2"), Integer.valueOf(2));
    assertEquals(testFieldCounts.get("value3"), Integer.valueOf(1));
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
  public void testMultiFieldCountByValue() throws Exception {
    // Setup test data
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Setup metadata for client-side partitioning
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0, 1); // key1->partition0, key2->partition1
    when(mockMetadata.getReplicas(1, 0)).thenReturn(Arrays.asList("server1:8080"));
    when(mockMetadata.getReplicas(1, 1)).thenReturn(Arrays.asList("server2:8080"));

    // Setup gRPC transport to return responses from different partitions
    ValueCount partition0Field1Counts =
        ValueCount.newBuilder().putValueToCounts("red", 6).putValueToCounts("blue", 3).build();
    ValueCount partition0Field2Counts =
        ValueCount.newBuilder().putValueToCounts("large", 4).putValueToCounts("small", 3).build();

    ValueCount partition1Field1Counts =
        ValueCount.newBuilder().putValueToCounts("red", 4).putValueToCounts("blue", 2).build();
    ValueCount partition1Field2Counts =
        ValueCount.newBuilder().putValueToCounts("large", 4).putValueToCounts("small", 4).build();

    CountByValueResponse partition0Response = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("color", partition0Field1Counts)
        .putFieldToValueCounts("size", partition0Field2Counts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    CountByValueResponse partition1Response = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("color", partition1Field1Counts)
        .putFieldToValueCounts("size", partition1Field2Counts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    when(mockGrpcTransportClient.countByValue(eq("server1:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(partition0Response));
    when(mockGrpcTransportClient.countByValue(eq("server2:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(partition1Response));

    // Execute the aggregation with multiple fields
    builder.countByValue(Arrays.asList("color", "size"), 5);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    // Verify the result
    AggregationResponse response = future.get();
    assertNotNull(response);
    assertFalse(response.hasError());

    Map<String, Map<String, Integer>> fieldCounts = response.getFieldToValueCounts();
    assertNotNull(fieldCounts);
    assertEquals(fieldCounts.size(), 2);

    // Verify color field counts (aggregated from both partitions)
    assertTrue(fieldCounts.containsKey("color"));
    Map<String, Integer> colorCounts = fieldCounts.get("color");
    assertEquals(colorCounts.get("red"), Integer.valueOf(10)); // 6+4
    assertEquals(colorCounts.get("blue"), Integer.valueOf(5)); // 3+2

    // Verify size field counts (aggregated from both partitions)
    assertTrue(fieldCounts.containsKey("size"));
    Map<String, Integer> sizeCounts = fieldCounts.get("size");
    assertEquals(sizeCounts.get("large"), Integer.valueOf(8)); // 4+4
    assertEquals(sizeCounts.get("small"), Integer.valueOf(7)); // 3+4
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

    // Setup metadata for client-side partitioning
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0);
    // Setup metadata to return no replicas for partition 0
    when(mockMetadata.getReplicas(1, 0)).thenReturn(Arrays.asList());

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

    // Setup metadata for client-side partitioning
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0);
    when(mockMetadata.getReplicas(1, 0)).thenReturn(Arrays.asList("server1:8080"));

    // Setup gRPC transport to return error response
    CountByValueResponse errorResponse = CountByValueResponse.newBuilder()
        .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
        .setErrorMessage("Server error")
        .build();

    when(mockGrpcTransportClient.countByValue(eq("server1:8080"), any(CountByValueRequest.class)))
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
      assertTrue(cause.getMessage().contains("Partition 0 aggregation failed"));
    }
  }

  @Test
  public void testClientSidePartitioning() throws Exception {
    // Setup test data with multiple keys
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3", "key4"));

    // Setup metadata for client-side partitioning - keys distributed across 2 partitions
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0, 1, 0, 1); // key1->partition0,
                                                                                        // key2->partition1,
                                                                                        // key3->partition0,
                                                                                        // key4->partition1
    when(mockMetadata.getReplicas(1, 0)).thenReturn(Arrays.asList("server1:8080"));
    when(mockMetadata.getReplicas(1, 1)).thenReturn(Arrays.asList("server2:8080"));

    // Setup gRPC transport to return responses from different partitions
    ValueCount partition0FieldCounts =
        ValueCount.newBuilder().putValueToCounts("electronics", 8).putValueToCounts("books", 2).build();

    ValueCount partition1FieldCounts =
        ValueCount.newBuilder().putValueToCounts("electronics", 4).putValueToCounts("books", 2).build();

    CountByValueResponse partition0Response = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("category", partition0FieldCounts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    CountByValueResponse partition1Response = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("category", partition1FieldCounts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    when(mockGrpcTransportClient.countByValue(eq("server1:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(partition0Response));
    when(mockGrpcTransportClient.countByValue(eq("server2:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(partition1Response));

    // Execute the aggregation
    builder.countByValue("category", 5);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    // Verify that the client made TWO calls (one to each partition server)
    verify(mockGrpcTransportClient, times(1)).countByValue(eq("server1:8080"), any(CountByValueRequest.class));
    verify(mockGrpcTransportClient, times(1)).countByValue(eq("server2:8080"), any(CountByValueRequest.class));

    // Verify the response contains the client-aggregated results
    AggregationResponse response = future.get();
    assertNotNull(response);
    assertFalse(response.hasError());

    Map<String, Integer> valueCounts = response.getValueCounts();
    assertEquals(valueCounts.size(), 2);
    assertEquals(valueCounts.get("electronics"), Integer.valueOf(12)); // 8+4 from both partitions
    assertEquals(valueCounts.get("books"), Integer.valueOf(4)); // 2+2 from both partitions
  }

  @Test
  public void testSinglePartitionAggregation() throws Exception {
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Setup metadata so both keys go to the same partition
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0, 0); // both keys go to partition 0
    when(mockMetadata.getReplicas(1, 0)).thenReturn(Arrays.asList("server1:8080"));

    ValueCount singleFieldCounts = ValueCount.newBuilder().putValueToCounts("test", 2).build();

    CountByValueResponse mockResponse = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("field", singleFieldCounts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    when(mockGrpcTransportClient.countByValue(eq("server1:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    builder.countByValue("field", 10);
    CompletableFuture<AggregationResponse> future = builder.execute(keys);

    // Verify single call to server (since all keys are in same partition)
    verify(mockGrpcTransportClient, times(1)).countByValue(eq("server1:8080"), any(CountByValueRequest.class));

    AggregationResponse response = future.get();
    assertNotNull(response);
    assertFalse(response.hasError());
    assertEquals(response.getValueCounts().get("test"), Integer.valueOf(2));
  }
}
