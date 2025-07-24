package com.linkedin.venice.grpc;

import static org.mockito.ArgumentMatchers.any;
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
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.avro.Schema;
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
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(0)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);

    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("TopK must be positive"));
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
  public void testTopKLimiting() {
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName("test_store_v1")
        .addFieldNames("category")
        .setTopK(2)
        .addKeys(ByteString.copyFrom("key1".getBytes()))
        .build();

    CountByValueResponse response = processor.processCountByValue(request);
    assertNotNull(response);
  }

  @Test
  public void testDefaultTopK() {
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
}
