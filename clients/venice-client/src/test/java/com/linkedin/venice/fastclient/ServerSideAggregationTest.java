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
import org.apache.avro.Schema;
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

    // Setup schema mock for validation
    String schemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"testField\",\"type\":\"string\"}" + "]}";
    Schema testSchema = new Schema.Parser().parse(schemaStr);
    when(mockMetadata.getLatestValueSchemaId()).thenReturn(1);
    when(mockMetadata.getValueSchema(1)).thenReturn(testSchema);

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
}
