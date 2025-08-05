package com.linkedin.venice.fastclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.GrpcTransportClient;
import com.linkedin.venice.protocols.BucketCount;
import com.linkedin.venice.protocols.BucketPredicate;
import com.linkedin.venice.protocols.ComparisonPredicate;
import com.linkedin.venice.protocols.CountByBucketRequest;
import com.linkedin.venice.protocols.CountByBucketResponse;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.protocols.ValueCount;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for server-side aggregation functionality.
 */
public class ServerSideAggregationTest {
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
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Setup metadata for client-side partitioning
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0, 1);
    when(mockMetadata.getReplica(any(Long.class), any(Integer.class), eq(1), eq(0), any(Set.class)))
        .thenReturn("server1:8080");
    when(mockMetadata.getReplica(any(Long.class), any(Integer.class), eq(1), eq(1), any(Set.class)))
        .thenReturn("server2:8080");

    // Setup gRPC transport responses
    ValueCount partition0Counts = ValueCount.newBuilder().putValueToCounts("value1", 3).build();
    ValueCount partition1Counts = ValueCount.newBuilder().putValueToCounts("value1", 2).build();

    CountByValueResponse response0 = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("testField", partition0Counts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    CountByValueResponse response1 = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("testField", partition1Counts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    when(mockGrpcTransportClient.countByValue(eq("server1:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response0));
    when(mockGrpcTransportClient.countByValue(eq("server2:8080"), any(CountByValueRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response1));

    // Execute and verify
    builder.countByValue(Arrays.asList("testField"), 10);
    AggregationResponse result = builder.execute(keys).get();

    assertNotNull(result);
    assertFalse(result.hasError());
    Map<String, Integer> counts = result.getFieldToValueCounts().get("testField");
    assertEquals(counts.get("value1"), Integer.valueOf(5)); // 3+2 aggregated
  }

  @Test
  public void testEmptyKeys() {
    builder.countByValue(Arrays.asList("testField"), 10);
    try {
      builder.execute(new HashSet<>());
      assert false : "Should throw exception";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("Keys cannot be null or empty"));
    }
  }

  @Test
  public void testAggregationResponseImpl() {
    ValueCount counts = ValueCount.newBuilder().putValueToCounts("value1", 5).build();
    CountByValueResponse grpcResponse = CountByValueResponse.newBuilder()
        .putFieldToValueCounts("testField", counts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    AggregationResponseImpl response = new AggregationResponseImpl(grpcResponse);
    assertFalse(response.hasError());
    assertEquals(response.getFieldToValueCounts().get("testField").get("value1"), Integer.valueOf(5));
  }

  @Test
  public void testSuccessfulCountByBucket() throws Exception {
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Setup metadata for client-side partitioning
    when(mockMetadata.getPartitionId(eq(1), any(byte[].class))).thenReturn(0, 1);
    when(mockMetadata.getReplica(any(Long.class), any(Integer.class), eq(1), eq(0), any(Set.class)))
        .thenReturn("server1:8080");
    when(mockMetadata.getReplica(any(Long.class), any(Integer.class), eq(1), eq(1), any(Set.class)))
        .thenReturn("server2:8080");

    // Setup bucket predicates
    Map<String, BucketPredicate> bucketPredicates = new HashMap<>();
    BucketPredicate gtPredicate = BucketPredicate.newBuilder()
        .setComparison(ComparisonPredicate.newBuilder().setOperator("GT").setFieldType("INT").setValue("10").build())
        .build();
    bucketPredicates.put("greater_than_10", gtPredicate);

    // Setup gRPC transport responses
    BucketCount partition0Counts = BucketCount.newBuilder().putBucketToCounts("greater_than_10", 3).build();
    BucketCount partition1Counts = BucketCount.newBuilder().putBucketToCounts("greater_than_10", 2).build();

    CountByBucketResponse response0 = CountByBucketResponse.newBuilder()
        .putFieldToBucketCounts("age", partition0Counts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    CountByBucketResponse response1 = CountByBucketResponse.newBuilder()
        .putFieldToBucketCounts("age", partition1Counts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    when(mockGrpcTransportClient.countByBucket(eq("server1:8080"), any(CountByBucketRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response0));
    when(mockGrpcTransportClient.countByBucket(eq("server2:8080"), any(CountByBucketRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(response1));

    // Execute and verify
    builder.countByBucket(Arrays.asList("age"), bucketPredicates);
    AggregationResponse result = builder.execute(keys).get();

    assertNotNull(result);
    assertFalse(result.hasError());
    Map<String, Integer> bucketCounts = result.getFieldToBucketCounts().get("age");
    assertEquals(bucketCounts.get("greater_than_10"), Integer.valueOf(5)); // 3+2 aggregated
  }

  @Test
  public void testCountByBucketWithEmptyPredicated() {
    try {
      builder.countByBucket(Arrays.asList("age"), new HashMap<>());
      assert false : "Should throw exception";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("Bucket predicates cannot be null or empty"));
    }
  }

  @Test
  public void testCountByBucketWithEmptyFieldNames() {
    Map<String, BucketPredicate> bucketPredicates = new HashMap<>();
    BucketPredicate gtPredicate = BucketPredicate.newBuilder()
        .setComparison(ComparisonPredicate.newBuilder().setOperator("GT").setFieldType("INT").setValue("10").build())
        .build();
    bucketPredicates.put("greater_than_10", gtPredicate);

    try {
      builder.countByBucket(Arrays.asList(), bucketPredicates);
      assert false : "Should throw exception";
    } catch (VeniceClientException e) {
      assertTrue(e.getMessage().contains("Field names cannot be null or empty"));
    }
  }

  @Test
  public void testAggregationResponseImplWithBuckets() {
    BucketCount bucketCounts = BucketCount.newBuilder().putBucketToCounts("bucket1", 5).build();
    CountByBucketResponse grpcResponse = CountByBucketResponse.newBuilder()
        .putFieldToBucketCounts("testField", bucketCounts)
        .setErrorCode(VeniceReadResponseStatus.OK)
        .build();

    AggregationResponseImpl response = new AggregationResponseImpl(grpcResponse);
    assertFalse(response.hasError());
    assertEquals(response.getFieldToBucketCounts().get("testField").get("bucket1"), Integer.valueOf(5));
  }

  @Test
  public void testAggregationResponseImplWithError() {
    CountByBucketResponse grpcResponse = CountByBucketResponse.newBuilder()
        .setErrorCode(VeniceReadResponseStatus.BAD_REQUEST)
        .setErrorMessage("Test error")
        .build();

    AggregationResponseImpl response = new AggregationResponseImpl(grpcResponse);
    assertTrue(response.hasError());
    assertEquals(response.getErrorMessage(), "Test error");
  }
}
