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
import java.util.HashSet;
import java.util.Set;
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

  // Test constants
  private static final String DEFAULT_STORE_NAME = "test_store";
  private static final String DEFAULT_RESOURCE_NAME = "test_store_v1";
  private static final String DEFAULT_FIELD_NAME = "category";
  private static final int DEFAULT_TOP_K = 5;
  private static final String DEFAULT_KEY = "key1";

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

  /**
   * Creates a CountByValueRequest with customizable parameters.
   */
  private CountByValueRequest createRequest(
      String resourceName,
      String[] fieldNames,
      int topK,
      String[] keys,
      int partition) {
    CountByValueRequest.Builder builder = CountByValueRequest.newBuilder()
        .setResourceName(resourceName != null ? resourceName : DEFAULT_RESOURCE_NAME)
        .setTopK(topK)
        .setPartition(partition);

    if (fieldNames != null && fieldNames.length > 0) {
      for (String fieldName: fieldNames) {
        builder.addFieldNames(fieldName);
      }
    }

    if (keys != null && keys.length > 0) {
      for (String key: keys) {
        builder.addKeys(ByteString.copyFrom(key.getBytes()));
      }
    }

    return builder.build();
  }

  // Convenience methods using the unified creator
  private CountByValueRequest createDefaultRequest() {
    return createRequest(null, new String[] { DEFAULT_FIELD_NAME }, DEFAULT_TOP_K, new String[] { DEFAULT_KEY }, 0);
  }

  private CountByValueRequest createRequestWithResourceName(String resourceName) {
    return createRequest(
        resourceName,
        new String[] { DEFAULT_FIELD_NAME },
        DEFAULT_TOP_K,
        new String[] { DEFAULT_KEY },
        0);
  }

  private CountByValueRequest createRequestWithFieldNames(String... fieldNames) {
    return createRequest(null, fieldNames, DEFAULT_TOP_K, new String[] { DEFAULT_KEY }, 0);
  }

  private CountByValueRequest createRequestWithTopK(int topK) {
    return createRequest(null, new String[] { DEFAULT_FIELD_NAME }, topK, new String[] { DEFAULT_KEY }, 0);
  }

  private CountByValueRequest createRequestWithNoFields() {
    return createRequest(null, null, DEFAULT_TOP_K, new String[] { DEFAULT_KEY }, 0);
  }

  private CountByValueRequest createRequestWithNoKeys() {
    return createRequest(null, new String[] { DEFAULT_FIELD_NAME }, DEFAULT_TOP_K, null, 0);
  }

  /**
   * Common assertion helper for successful responses.
   */
  private void assertSuccessResponse(CountByValueResponse response) {
    assertNotNull(response);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
  }

  /**
   * Setup storage engine with multiple partitions.
   */
  private void setupStorageWithPartitions(Set<Integer> partitions) {
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitions);
    for (Integer partition: partitions) {
      when(mockStorageEngine.get(eq(partition), any(byte[].class))).thenReturn(null);
    }
  }

  @Test
  public void testSuccessfulCountByValue() throws Exception {
    CountByValueResponse response = processor.processCountByValue(createDefaultRequest());
    assertSuccessResponse(response);
  }

  @Test(dataProvider = "invalidRequestDataProvider")
  public void testInvalidRequests(CountByValueRequest request, int expectedCode, String expectedMessage) {
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), expectedCode);
    assertTrue(response.getErrorMessage().contains(expectedMessage));
  }

  @org.testng.annotations.DataProvider
  public Object[][] invalidRequestDataProvider() {
    return new Object[][] {
        { createRequestWithNoFields(), VeniceReadResponseStatus.BAD_REQUEST, "Field names cannot be null or empty" },
        { createRequestWithResourceName("invalid_format"), VeniceReadResponseStatus.BAD_REQUEST,
            "Invalid resource name format" },
        { createRequestWithFieldNames("nonexistent_field"), VeniceReadResponseStatus.BAD_REQUEST,
            "Field 'nonexistent_field' not found in schema" } };
  }

  @Test
  public void testStoreNotFound() {
    when(mockStoreRepository.getStoreOrThrow("nonexistent_store"))
        .thenThrow(new VeniceNoStoreException("nonexistent_store"));

    CountByValueResponse response =
        processor.processCountByValue(createRequestWithResourceName("nonexistent_store_v1"));
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Store not found"));
  }

  @Test
  public void testStorageEngineNotFound() {
    when(mockStorageEngineRepository.getLocalStorageEngine("test_store_v1")).thenReturn(null);

    CountByValueResponse response = processor.processCountByValue(createDefaultRequest());
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Storage engine not found"));
  }

  @Test(dataProvider = "topKVariationsDataProvider")
  public void testTopKVariations(int topK, String description) {
    CountByValueRequest request = createRequestWithTopK(topK);
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.OK);
  }

  @org.testng.annotations.DataProvider
  public Object[][] topKVariationsDataProvider() {
    return new Object[][] { { 0, "Zero TopK" }, { 2, "Small TopK" }, { Integer.MAX_VALUE, "Large TopK" },
        { -1, "Negative TopK" } };
  }

  @Test
  public void testMultipleKeys() {
    CountByValueRequest request =
        createRequest(null, new String[] { DEFAULT_FIELD_NAME }, 3, new String[] { "key1", "key2" }, 0);
    CountByValueResponse response = processor.processCountByValue(request);
    assertSuccessResponse(response);
  }

  @Test
  public void testEmptyKeysList() {
    CountByValueResponse response = processor.processCountByValue(createRequestWithNoKeys());
    assertSuccessResponse(response);
  }

  @Test
  public void testInvalidVersionNumber() {
    CountByValueRequest request = createRequestWithResourceName("test_store_vabc");
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Invalid resource name format"));
  }

  @Test
  public void testStoreVersionNotExist() {
    when(mockStore.containsVersion(999)).thenReturn(false);
    CountByValueRequest request = createRequestWithResourceName("test_store_v999");
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Store version 999 does not exist"));
  }

  @Test
  public void testNoValueSchema() {
    when(mockSchemaRepository.getSupersetOrLatestValueSchema("test_store")).thenReturn(null);
    CountByValueRequest request = createDefaultRequest();
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("No value schema found"));
  }

  @Test
  public void testStoreVersionStateNotAvailable() {
    when(mockStorageEngine.getStoreVersionState()).thenReturn(null);
    CountByValueRequest request = createDefaultRequest();
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("Store version state not available"));
  }

  @Test
  public void testGetVersionFailure() {
    when(mockStore.containsVersion(1)).thenReturn(false);
    when(mockStore.getVersion(1)).thenReturn(null);
    CountByValueRequest request = createDefaultRequest();
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("Store version 1 does not exist"));
  }

  @Test
  public void testMultiplePartitions() {
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    partitions.add(1);
    partitions.add(2);
    setupStorageWithPartitions(partitions);

    CountByValueRequest request =
        createRequest("test_store_v1", new String[] { "category" }, 5, new String[] { "key1" }, 0);
    CountByValueResponse response = processor.processCountByValue(request);

    assertSuccessResponse(response);
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
  }

  @Test
  public void testEmptyPartitionSet() {
    setupStorageWithPartitions(new HashSet<>());

    CountByValueRequest request =
        createRequest("test_store_v1", new String[] { "category" }, 5, new String[] { "key1" }, 0);
    CountByValueResponse response = processor.processCountByValue(request);

    assertSuccessResponse(response);
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
    assertTrue(response.getFieldToValueCountsMap().get("category").getValueToCountsMap().isEmpty());
  }

  @Test
  public void testKeyFoundInSecondPartition() {
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    partitions.add(1);
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitions);
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenReturn(null);
    when(mockStorageEngine.get(eq(1), any(byte[].class))).thenReturn("test_record_data".getBytes());

    CountByValueRequest request =
        createRequest("test_store_v1", new String[] { "category" }, 5, new String[] { "key1" }, 0);
    CountByValueResponse response = processor.processCountByValue(request);

    assertSuccessResponse(response);
  }

  @Test
  public void testExceptionDuringStorageGet() {
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    partitions.add(1);
    when(mockStorageEngine.getPersistedPartitionIds()).thenReturn(partitions);
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenThrow(new RuntimeException("Storage error"));
    when(mockStorageEngine.get(eq(1), any(byte[].class))).thenReturn(null);

    CountByValueRequest request =
        createRequest("test_store_v1", new String[] { "category" }, 5, new String[] { "key1" }, 0);
    CountByValueResponse response = processor.processCountByValue(request);

    assertSuccessResponse(response);
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
    assertTrue(response.getFieldToValueCountsMap().get("category").getValueToCountsMap().isEmpty());
  }

  @Test
  public void testStorageEngineException() {
    when(mockStorageEngineRepository.getLocalStorageEngine("test_store_v1"))
        .thenThrow(new RuntimeException("Storage not found"));

    CountByValueResponse response = processor.processCountByValue(createDefaultRequest());
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("Internal error"));
  }

  @Test
  public void testDefaultConstructorProcessCountByValue() {
    VeniceServerGrpcRequestProcessor defaultProcessor = new VeniceServerGrpcRequestProcessor();
    CountByValueResponse response = defaultProcessor.processCountByValue(createDefaultRequest());
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("CountByValue dependencies not available"));
  }

  @Test
  public void testStringSchemaSupport() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    when(mockSchemaEntry.getSchema()).thenReturn(stringSchema);

    CountByValueRequest request = createRequestWithFieldNames("value");
    CountByValueResponse response = processor.processCountByValue(request);
    assertSuccessResponse(response);
  }

  @Test
  public void testStringSchemaValueField() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    when(mockSchemaEntry.getSchema()).thenReturn(stringSchema);

    CountByValueRequest request = createRequestWithFieldNames("_value");
    CountByValueResponse response = processor.processCountByValue(request);
    assertSuccessResponse(response);
  }

  @Test
  public void testStringSchemaInvalidField() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    when(mockSchemaEntry.getSchema()).thenReturn(stringSchema);

    CountByValueRequest request = createRequestWithFieldNames("invalid_field");
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("only 'value' or '_value' field names are supported"));
  }

  @Test
  public void testUnsupportedSchemaType() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    when(mockSchemaEntry.getSchema()).thenReturn(intSchema);

    CountByValueRequest request = createRequestWithFieldNames("value");
    CountByValueResponse response = processor.processCountByValue(request);
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.BAD_REQUEST);
    assertTrue(response.getErrorMessage().contains("CountByValue only supports STRING and RECORD value types"));
  }

  @Test
  public void testHandlerManagement() {
    VeniceServerGrpcRequestProcessor processor = new VeniceServerGrpcRequestProcessor();
    com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler handler1 =
        mock(com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler.class);
    com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler handler2 =
        mock(com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler.class);
    com.linkedin.venice.listener.grpc.GrpcRequestContext mockContext =
        mock(com.linkedin.venice.listener.grpc.GrpcRequestContext.class);

    when(handler1.getNext()).thenReturn(null);

    // Test adding handlers and processing requests
    processor.addHandler(handler1);
    processor.addHandler(handler2);
    processor.processRequest(mockContext);

    // Test processing with no handlers
    new VeniceServerGrpcRequestProcessor().processRequest(mockContext);

    assertNotNull(processor);
  }

  @Test
  public void testNullDependenciesInConstructor() {
    VeniceServerGrpcRequestProcessor processor = new VeniceServerGrpcRequestProcessor(null, null, null, null, null);
    CountByValueResponse response = processor.processCountByValue(createDefaultRequest());
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("CountByValue dependencies not available"));
  }

  @Test
  public void testWithCompressionEnabled() throws Exception {
    mockStoreVersionState.compressionStrategy = com.linkedin.venice.compression.CompressionStrategy.GZIP.getValue();

    com.linkedin.venice.compression.VeniceCompressor gzipCompressor =
        mock(com.linkedin.venice.compression.VeniceCompressor.class);
    when(mockCompressorFactory.getCompressor(com.linkedin.venice.compression.CompressionStrategy.GZIP))
        .thenReturn(gzipCompressor);

    java.nio.ByteBuffer decompressedBuffer = java.nio.ByteBuffer.wrap("decompressed_data".getBytes());
    when(gzipCompressor.decompress(any(java.nio.ByteBuffer.class))).thenReturn(decompressedBuffer);

    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    setupStorageWithPartitions(partitions);
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenReturn("compressed_data".getBytes());

    CountByValueResponse response = processor.processCountByValue(createDefaultRequest());
    assertSuccessResponse(response);
  }

  @Test
  public void testPartialNullDependencies() {
    VeniceServerGrpcRequestProcessor processor = new VeniceServerGrpcRequestProcessor(
        null,
        mockSchemaRepository,
        mockStoreRepository,
        mockExecutor,
        mockCompressorFactory);

    CountByValueResponse response = processor.processCountByValue(createDefaultRequest());
    assertEquals(response.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR);
    assertTrue(response.getErrorMessage().contains("CountByValue dependencies not available"));
  }

  @Test
  public void testHandlerChainTraversal() {
    VeniceServerGrpcRequestProcessor processor = new VeniceServerGrpcRequestProcessor();
    com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler handler1 =
        mock(com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler.class);
    com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler handler2 =
        mock(com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler.class);
    com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler handler3 =
        mock(com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler.class);

    when(handler1.getNext()).thenReturn(handler2);
    when(handler2.getNext()).thenReturn(null);

    processor.addHandler(handler1);
    processor.addHandler(handler3);

    assertNotNull(processor);
  }

  @Test
  public void testDeserializationError() {
    Set<Integer> partitions = new HashSet<>();
    partitions.add(0);
    setupStorageWithPartitions(partitions);
    when(mockStorageEngine.get(eq(0), any(byte[].class))).thenReturn(new byte[] { 1, 2, 3, 4, 5 }); // Invalid Avro data

    CountByValueResponse response = processor.processCountByValue(createDefaultRequest());
    assertSuccessResponse(response);
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
  }

  @Test
  public void testMultipleFieldNames() {
    CountByValueRequest request = createRequestWithFieldNames("category", "value");
    CountByValueResponse response = processor.processCountByValue(request);

    assertSuccessResponse(response);
    assertTrue(response.getFieldToValueCountsMap().containsKey("category"));
    assertTrue(response.getFieldToValueCountsMap().containsKey("value"));
  }
}
