package com.linkedin.venice.grpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcRequestProcessor;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test for VeniceServerGrpcRequestProcessor countByValue functionality.
 */
public class VeniceServerGrpcRequestProcessorTest {
  private StorageEngineRepository mockStorageEngineRepository;
  private ReadOnlySchemaRepository mockSchemaRepository;
  private ReadOnlyStoreRepository mockStoreRepository;
  private ThreadPoolExecutor mockExecutor;
  private StorageEngineBackedCompressorFactory mockCompressorFactory;
  private StorageEngine mockStorageEngine;
  private Store mockStore;
  private SchemaEntry mockSchemaEntry;
  private StoreVersionState mockStoreVersionState;
  private VeniceCompressor mockCompressor;
  private RecordDeserializer<GenericRecord> mockDeserializer;
  private AvroStoreDeserializerCache mockDeserializerCache;
  private VeniceServerGrpcRequestProcessor processor;

  @BeforeMethod
  public void setUp() throws Exception {
    // Create mocks
    mockStorageEngineRepository = mock(StorageEngineRepository.class);
    mockSchemaRepository = mock(ReadOnlySchemaRepository.class);
    mockStoreRepository = mock(ReadOnlyStoreRepository.class);
    mockExecutor = mock(ThreadPoolExecutor.class);
    mockCompressorFactory = mock(StorageEngineBackedCompressorFactory.class);
    mockStorageEngine = mock(StorageEngine.class);
    mockStore = mock(Store.class);
    mockSchemaEntry = mock(SchemaEntry.class);
    mockStoreVersionState = mock(StoreVersionState.class);
    mockCompressor = mock(VeniceCompressor.class);
    mockDeserializer = mock(RecordDeserializer.class);
    mockDeserializerCache = mock(AvroStoreDeserializerCache.class);

    // Setup schema
    String schemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"category\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"int\"}]}";
    Schema schema = Schema.parse(schemaStr);
    when(mockSchemaEntry.getSchema()).thenReturn(schema);
    when(mockSchemaEntry.getId()).thenReturn(1);

    // Setup store and version
    when(mockStore.containsVersion(1)).thenReturn(true);
    Version mockVersion = mock(Version.class);
    when(mockStore.getVersion(1)).thenReturn(mockVersion);
    // Mock partition config instead of partitioner directly since Version doesn't have getPartitioner()
    when(mockVersion.getPartitionCount()).thenReturn(4);

    // Setup storage engine
    when(mockStorageEngine.getStoreVersionState()).thenReturn(mockStoreVersionState);

    // Setup compression - mock the static utility method call results
    when(mockCompressorFactory.getCompressor(CompressionStrategy.NO_OP)).thenReturn(mockCompressor);

    // Setup repositories
    when(mockStoreRepository.getStoreOrThrow("test_store")).thenReturn(mockStore);
    when(mockSchemaRepository.getSupersetOrLatestValueSchema("test_store")).thenReturn(mockSchemaEntry);
    when(mockStorageEngineRepository.getLocalStorageEngine("test_store_v1")).thenReturn(mockStorageEngine);

    // Setup executor to run tasks immediately (synchronously)
    doAnswer(invocation -> {
      Runnable task = invocation.getArgument(0);
      task.run(); // Execute immediately on the same thread
      return null;
    }).when(mockExecutor).execute(any(Runnable.class));

    // Create processor with constructor that accepts dependencies directly
    processor = new VeniceServerGrpcRequestProcessor(
        mockStorageEngineRepository,
        mockSchemaRepository,
        mockStoreRepository,
        mockExecutor,
        mockCompressorFactory);
  }

  @Test
  public void testSuccessfulCountByValue() throws Exception {
    // Simple test - just verify it doesn't crash
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);
    assertNotNull(response);
    // Test basic functionality without complex mocking
  }

  @Test
  public void testEmptyFieldNames() {
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Field names cannot be null or empty"));
  }

  @Test
  public void testInvalidTopK() {
    // Note: Server no longer validates TopK since client handles TopK filtering
    // This test is kept for backward compatibility but server accepts any TopK value
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(0)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);
    // Server no longer validates TopK - it returns all counts and client does TopK filtering
    assertNotNull(response);
  }

  @Test
  public void testInvalidResourceName() {
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("invalid_format")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Invalid resource name format"));
  }

  @Test
  public void testStoreNotFound() {
    when(mockStoreRepository.getStoreOrThrow("nonexistent_store"))
        .thenThrow(new VeniceNoStoreException("nonexistent_store"));

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("nonexistent_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Store not found"));
  }

  @Test
  public void testFieldNotFound() {
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("nonexistent_field")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Field 'nonexistent_field' not found"));
  }

  @Test
  public void testStorageEngineNotFound() {
    when(mockStorageEngineRepository.getLocalStorageEngine("test_store_v1")).thenReturn(null);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Storage engine not found"));
  }

  @Test
  public void testMultipleKeys() {
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(3)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .addKeys(ByteString.copyFrom("key2".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);
    assertNotNull(response);
  }

  @Test
  public void testServerReturnsAllCounts() {
    // Server no longer does TopK limiting - it returns all counts for client-side aggregation
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(2)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);
    assertNotNull(response);
    // Server returns all counts, client will apply TopK=2 filtering
  }

  @Test
  public void testDefaultTopK() {
    // Server ignores TopK value since client handles TopK filtering
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);
    assertNotNull(response);
  }

  @Test
  public void testEmptyKeysList() {
    CountByValueRequest request =
        CountByValueRequest.newBuilder().setResourceName("test_store_v1").addFieldNames("category").setTopK(5).build();

    CountByValueResponse response = processor.processCountByValue(request);
    assertNotNull(response);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
  }

  @Test
  public void testInvalidVersionNumber() {
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_vabc")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Invalid version number"));
  }

  @Test
  public void testStoreVersionNotExist() {
    when(mockStore.containsVersion(999)).thenReturn(false);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v999")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Store version 999 does not exist"));
  }

  @Test
  public void testNoValueSchema() {
    when(mockSchemaRepository.getSupersetOrLatestValueSchema("test_store")).thenReturn(null);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("No value schema found"));
  }

  @Test
  public void testStoreVersionStateNotAvailable() {
    when(mockStorageEngine.getStoreVersionState()).thenReturn(null);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("Store version state not available"));
  }

  @Test
  public void testGetVersionFailure() {
    // Both containsVersion and getVersion should be consistent
    when(mockStore.containsVersion(1)).thenReturn(false);
    when(mockStore.getVersion(1)).thenReturn(null);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Store version 1 does not exist"));
  }

  @Test
  public void testMultiplePartitions() {
    // Test with multiple partitions to increase coverage of partition iteration
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    partitions.add(1);
    partitions.add(2);
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitions);
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenReturn(null);
    when(mockStorageEngine.get(eq(1), any(byte[].class))).thenReturn(null);
    when(mockStorageEngine.get(eq(2), any(byte[].class))).thenReturn(null);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
  }

  @Test
  public void testEmptyPartitionSet() {
    // Setup storage engine with no persisted partitions
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(new HashSet<>());

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
    // Should return empty counts when no partitions available
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
    assertTrue(response.getFieldToValueCountsMap().get("category").getValueToCountsMap().isEmpty());
  }

  @Test
  public void testKeyNotFoundInAnyPartition() {
    // Setup storage engine to return null for all partitions
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    partitions.add(1);
    partitions.add(2);
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitions);
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenReturn(null);
    when(mockStorageEngine.get(eq(1), any(byte[].class))).thenReturn(null);
    when(mockStorageEngine.get(eq(2), any(byte[].class))).thenReturn(null);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("nonexistent_key".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
    // Should return empty counts when key not found
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
    assertTrue(response.getFieldToValueCountsMap().get("category").getValueToCountsMap().isEmpty());
  }

  @Test
  public void testFoundKeyInSecondPartition() {
    // Test when key is found in second partition (to cover the partition loop)
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    partitions.add(1);
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitions);

    // Key not found in first partition, found in second
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenReturn(null);
    byte[] recordData = "test_record_data".getBytes();
    when(mockStorageEngine.get(eq(1), any(byte[].class))).thenReturn(recordData);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
  }

  @Test
  public void testExceptionDuringStorageGet() {
    // Test exception handling during storage engine get
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    partitions.add(1);
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitions);

    // First partition throws exception, second returns null
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenThrow(new RuntimeException("Storage error"));
    when(mockStorageEngine.get(eq(1), any(byte[].class))).thenReturn(null);

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
    // Should handle exception gracefully and continue
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
    // Should have empty counts since both partitions failed to return data
    assertTrue(response.getFieldToValueCountsMap().get("category").getValueToCountsMap().isEmpty());
  }

  @Test
  public void testStorageEngineException() {
    // Test when getLocalStorageEngine throws exception
    when(mockStorageEngineRepository.getLocalStorageEngine("test_store_v1"))
        .thenThrow(new RuntimeException("Storage not found"));

    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(5)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("Internal error"));
  }
}
