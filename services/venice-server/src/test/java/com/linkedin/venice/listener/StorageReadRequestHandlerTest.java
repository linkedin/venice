package com.linkedin.venice.listener;

import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.MetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.store.AvroComputeRequestBuilderV3;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.NoopCompressor;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.AdminRequest;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.response.ComputeResponseWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ServerAdminAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.utils.DataProviderUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StorageReadRequestHandlerTest {
  private static class InlineExecutor extends ThreadPoolExecutor {
    public InlineExecutor() {
      super(0, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    @Override
    public void execute(Runnable runnable) {
      runnable.run();
    }
  }

  private final ChannelHandlerContext context = mock(ChannelHandlerContext.class);
  private final ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
  private final ThreadPoolExecutor executor = new InlineExecutor();
  private final Store store = mock(Store.class);
  private final Version version = mock(Version.class);
  private final AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
  private final StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
  private final ReadOnlyStoreRepository storeRepository = mock(ReadOnlyStoreRepository.class);
  private final ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
  private final StorageEngineBackedCompressorFactory compressorFactory =
      mock(StorageEngineBackedCompressorFactory.class);
  private final DiskHealthCheckService healthCheckService = mock(DiskHealthCheckService.class);
  private final MetadataRetriever metadataRetriever = mock(MetadataRetriever.class);
  private final VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
  private final VenicePartitioner partitioner = new SimplePartitioner();
  private static final int amplificationFactor = 3;

  @BeforeMethod
  public void setUp() {
    doReturn(store).when(storeRepository).getStoreOrThrow(any());
    doReturn(Optional.of(version)).when(store).getVersion(anyInt());

    doReturn("test-store_v1").when(version).kafkaTopicName();
    PartitionerConfig partitionerConfig =
        new PartitionerConfigImpl(partitioner.getClass().getName(), Collections.emptyMap(), amplificationFactor);
    doReturn(partitionerConfig).when(version).getPartitionerConfig();

    doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(any());
    doReturn(new NoopCompressor()).when(compressorFactory).getCompressor(any(), any());

    RocksDBServerConfig rocksDBServerConfig = mock(RocksDBServerConfig.class);
    doReturn(rocksDBServerConfig).when(serverConfig).getRocksDBServerConfig();
  }

  @AfterMethod
  public void cleanUp() {
    Mockito.reset(
        storageEngine,
        storageEngineRepository,
        storeRepository,
        schemaRepository,
        compressorFactory,
        healthCheckService,
        metadataRetriever,
        serverConfig,
        context);
  }

  private StorageReadRequestHandler createStorageReadRequestHandler() {
    return createStorageReadRequestHandler(false, 0);
  }

  private StorageReadRequestHandler createStorageReadRequestHandler(
      boolean parallelBatchGetEnabled,
      int parallelBatchGetChunkSize) {
    return new StorageReadRequestHandler(
        executor,
        executor,
        storageEngineRepository,
        storeRepository,
        schemaRepository,
        metadataRetriever,
        healthCheckService,
        false,
        parallelBatchGetEnabled,
        parallelBatchGetChunkSize,
        serverConfig,
        compressorFactory,
        Optional.empty());
  }

  private int getSubPartitionId(int partition, byte[] keyBytes) {
    return partition * amplificationFactor + partitioner.getPartitionId(keyBytes, amplificationFactor);
  }

  @Test
  public void storageExecutionHandlerPassesRequestsAndGeneratesResponses() throws Exception {
    String keyString = "test-key";
    String valueString = "test-value";
    int schemaId = 1;
    int partition = 2;
    byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
    int subPartition = getSubPartitionId(partition, keyString.getBytes());
    doReturn(valueBytes).when(storageEngine).get(subPartition, ByteBuffer.wrap(keyString.getBytes()));

    // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
    String uri = "/" + TYPE_STORAGE + "/test-topic_v1/" + partition + "/" + keyString;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    GetRouterRequest request = GetRouterRequest.parseGetHttpRequest(httpRequest);

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    StorageResponseObject responseObject = (StorageResponseObject) argumentCaptor.getValue();
    Assert.assertEquals(responseObject.getValueRecord().getDataInBytes(), valueString.getBytes());
    Assert.assertEquals(responseObject.getValueRecord().getSchemaId(), schemaId);
  }

  @Test
  public void testDiskHealthCheckService() throws Exception {
    doReturn(true).when(healthCheckService).isDiskHealthy();

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, new HealthCheckRequest());

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    HttpShortcutResponse healthCheckResponse = (HttpShortcutResponse) argumentCaptor.getValue();
    Assert.assertEquals(healthCheckResponse.getStatus(), HttpResponseStatus.OK);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testMultiGetNotUsingKeyBytes(Boolean isParallel) throws Exception {
    int schemaId = 1;

    // [0]""/[1]"storage"/[2]{$resourceName}
    String uri = "/" + TYPE_STORAGE + "/test-topic_v1";

    RecordSerializer<MultiGetRouterRequestKeyV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);
    List<MultiGetRouterRequestKeyV1> keys = new ArrayList<>();
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    String keyPrefix = "key_";
    String valuePrefix = "value_";

    Map<Integer, String> allValueStrings = new HashMap<>();
    int recordCount = 10;

    // Prepare multiGet records belong to specific sub-partitions, if the router does not have the right logic to figure
    // out
    // the sub-partition, the test will fail. Here we use SimplePartitioner to generate sub-partition id, as it only
    // considers the first byte of the key.
    for (int i = 0; i < recordCount; ++i) {
      MultiGetRouterRequestKeyV1 requestKey = new MultiGetRouterRequestKeyV1();
      byte[] keyBytes = keySerializer.serialize(null, keyPrefix + i);
      int subPartition = partitioner.getPartitionId(keyBytes, amplificationFactor);
      requestKey.keyBytes = ByteBuffer.wrap(keyBytes);
      requestKey.keyIndex = i;
      requestKey.partitionId = 0;
      String valueString = valuePrefix + i;
      byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
      doReturn(valueBytes).when(storageEngine).get(subPartition, ByteBuffer.wrap(keyBytes));
      allValueStrings.put(i, valueString);
      keys.add(requestKey);
    }

    // Prepare request
    byte[] postBody = serializer.serializeObjects(keys);
    FullHttpRequest httpRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri, Unpooled.wrappedBuffer(postBody));
    httpRequest.headers()
        .set(
            HttpConstants.VENICE_API_VERSION,
            ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion());
    MultiGetRouterRequestWrapper request = MultiGetRouterRequestWrapper.parseMultiGetHttpRequest(httpRequest);

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler(isParallel, 10);
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    MultiGetResponseWrapper multiGetResponseWrapper = (MultiGetResponseWrapper) argumentCaptor.getValue();
    RecordDeserializer<MultiGetResponseRecordV1> deserializer =
        SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);
    Iterable<MultiGetResponseRecordV1> values =
        deserializer.deserializeObjects(multiGetResponseWrapper.getResponseBody().array());
    Map<Integer, String> results = new HashMap<>();
    values.forEach(K -> {
      String valueString = new String(K.value.array(), StandardCharsets.UTF_8);
      results.put(K.keyIndex, valueString);
    });
    Assert.assertEquals(results.size(), recordCount);
    for (int i = 0; i < recordCount; i++) {
      Assert.assertEquals(results.get(i), allValueStrings.get(i));
    }
  }

  @Test
  public void storageExecutionHandlerLogsExceptions() throws Exception {
    String topic = "temp-test-topic_v1";
    String keyString = "testkey";
    String valueString = "testvalue";
    int schemaId = 1;
    int partition = 3;

    // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
    String uri = "/" + TYPE_STORAGE + "/" + topic + "/" + partition + "/" + keyString;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    GetRouterRequest request = GetRouterRequest.parseGetHttpRequest(httpRequest);

    byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
    int subPartition = getSubPartitionId(partition, keyString.getBytes());
    doReturn(valueBytes).when(storageEngine).get(subPartition, ByteBuffer.wrap(keyString.getBytes()));

    String exceptionMessage = "Exception thrown in Mock";
    // Forcing an exception to be thrown.
    doThrow(new VeniceException(exceptionMessage)).when(storageEngineRepository).getLocalStorageEngine(topic);

    AtomicInteger errorLogCount = new AtomicInteger();
    // Adding a custom appender to track the count of error logs we are interested in
    ErrorCountAppender errorCountAppender = new ErrorCountAppender.Builder().setErrorMessageCounter(errorLogCount)
        .setExceptionMessage(exceptionMessage)
        .build();
    errorCountAppender.start();

    LoggerContext ctx = ((LoggerContext) LogManager.getContext(false));
    Configuration config = ctx.getConfiguration();
    config.addLoggerAppender(
        (org.apache.logging.log4j.core.Logger) LogManager.getLogger(StorageReadRequestHandler.class),
        errorCountAppender);

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    HttpShortcutResponse shortcutResponse = (HttpShortcutResponse) argumentCaptor.getValue();
    Assert.assertEquals(shortcutResponse.getStatus(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
    Assert.assertEquals(shortcutResponse.getMessage(), exceptionMessage);

    // Asserting that the exception got logged
    Assert.assertTrue(errorLogCount.get() > 0);
  }

  @Test
  public void testAdminRequestsPassInStorageExecutionHandler() throws Exception {
    String topic = "test_store_v1";
    int expectedPartitionId = 12345;

    // [0]""/[1]"action"/[2]"store_version"/[3]"dump_ingestion_state"
    String uri =
        "/" + QueryAction.ADMIN.toString().toLowerCase() + "/" + topic + "/" + ServerAdminAction.DUMP_INGESTION_STATE;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    AdminRequest request = AdminRequest.parseAdminHttpRequest(httpRequest);

    // Mock the AdminResponse from ingestion task
    AdminResponse expectedAdminResponse = new AdminResponse();
    PartitionConsumptionState state = new PartitionConsumptionState(
        expectedPartitionId,
        1,
        new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer()),
        false);
    expectedAdminResponse.addPartitionConsumptionState(state);
    doReturn(expectedAdminResponse).when(metadataRetriever).getConsumptionSnapshots(eq(topic), any());

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    AdminResponse adminResponse = (AdminResponse) argumentCaptor.getValue();
    Assert.assertEquals(adminResponse.getResponseRecord().partitionConsumptionStates.size(), 1);
    Assert.assertEquals(
        adminResponse.getResponseRecord().partitionConsumptionStates.get(0).partitionId,
        expectedPartitionId);
    Assert.assertEquals(
        adminResponse.getResponseSchemaIdHeader(),
        AvroProtocolDefinition.SERVER_ADMIN_RESPONSE_V1.getCurrentProtocolVersion());
  }

  @Test
  public void testMetadataFetchRequestsPassInStorageExecutionHandler() throws Exception {
    String storeName = "test_store_name";
    Map<CharSequence, CharSequence> keySchema = Collections.singletonMap("test_key_schema_id", "test_key_schema");
    Map<CharSequence, CharSequence> valueSchemas =
        Collections.singletonMap("test_value_schema_id", "test_value_schemas");

    // [0]""/[1]"action"/[2]"store"
    String uri = "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    MetadataFetchRequest testRequest = MetadataFetchRequest.parseGetHttpRequest(httpRequest);

    // Mock the MetadataResponse from ingestion task
    MetadataResponse expectedMetadataResponse = new MetadataResponse();
    VersionProperties versionProperties = new VersionProperties(
        1,
        0,
        1,
        "test_partitioner_class",
        Collections.singletonMap("test_partitioner_param", "test_param"),
        2);
    expectedMetadataResponse.setVersions(Collections.singletonList(1));
    expectedMetadataResponse.setVersionMetadata(versionProperties);
    expectedMetadataResponse.setKeySchema(keySchema);
    expectedMetadataResponse.setValueSchemas(valueSchemas);
    doReturn(expectedMetadataResponse).when(metadataRetriever).getMetadata(eq(storeName));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, testRequest);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    MetadataResponse metadataResponse = (MetadataResponse) argumentCaptor.getValue();
    Assert.assertEquals(metadataResponse.getResponseRecord().getVersionMetadata(), versionProperties);
    Assert.assertEquals(metadataResponse.getResponseRecord().getKeySchema(), keySchema);
    Assert.assertEquals(metadataResponse.getResponseRecord().getValueSchemas(), valueSchemas);
  }

  @Test
  public void testUnrecognizedRequestInStorageExecutionHandler() throws Exception {
    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, new Object());

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    HttpShortcutResponse shortcutResponse = (HttpShortcutResponse) argumentCaptor.getValue();
    Assert.assertEquals(shortcutResponse.getStatus(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
    Assert.assertEquals(shortcutResponse.getMessage(), "Unrecognized object in StorageExecutionHandler");
  }

  @Test
  public void testHandleComputeRequest() throws Exception {
    String keyString = "test-key";
    String missingKeyString = "missing-test-key";
    GenericRecord valueRecord = new GenericData.Record(
        SchemaBuilder.record("SampleSchema")
            .fields()
            .name("listField")
            .type()
            .array()
            .items()
            .floatType()
            .noDefault()
            .endRecord());
    valueRecord.put("listField", Collections.singletonList(1.0f));

    SchemaEntry schemaEntry = new SchemaEntry(1, valueRecord.getSchema());
    doReturn(schemaEntry).when(schemaRepository).getSupersetOrLatestValueSchema(any());
    doReturn(schemaEntry).when(schemaRepository).getValueSchema(any(), anyInt());

    int partition = 1;
    int subPartition = getSubPartitionId(partition, keyString.getBytes());
    AvroSerializer valueSerializer = new AvroSerializer<>(valueRecord.getSchema());
    byte[] valueBytes = ValueRecord.create(schemaEntry.getId(), valueSerializer.serialize(valueRecord)).serialize();
    doReturn(ByteBuffer.wrap(valueBytes)).when(storageEngine).get(eq(subPartition), eq(keyString.getBytes()), any());

    Set<Object> keySet = new HashSet<>(Arrays.asList(keyString, missingKeyString));
    AvroGenericReadComputeStoreClient storeClient = mock(AvroGenericReadComputeStoreClient.class);
    doReturn("test-store").when(storeClient).getStoreName();
    new AvroComputeRequestBuilderV3<>(storeClient, valueRecord.getSchema())
        .dotProduct("listField", Collections.singletonList(4.0f), "dotProduct")
        .hadamardProduct("listField", Collections.singletonList(5.0f), "hadamardProduct")
        .execute(keySet);
    ArgumentCaptor<ComputeRequestWrapper> requestCaptor = ArgumentCaptor.forClass(ComputeRequestWrapper.class);
    verify(storeClient, times(1)).compute(requestCaptor.capture(), any(), any(), any(), anyLong());
    ComputeRequestWrapper computeRequest = requestCaptor.getValue();
    // During normal operation, StorageReadRequestHandler gets a request after Avro deserialization,
    // which as a side effect converts String to Utf8.
    // Here we simulate this by serializing the request and then deserializing it back.
    computeRequest.deserialize(
        OptimizedBinaryDecoderFactory.defaultFactory()
            .createOptimizedBinaryDecoder(ByteBuffer.wrap(computeRequest.serialize())));

    ComputeRouterRequestWrapper request = mock(ComputeRouterRequestWrapper.class);
    doReturn(RequestType.COMPUTE).when(request).getRequestType();
    doReturn(true).when(request).isStreamingRequest();
    doReturn(schemaEntry.getId()).when(request).getValueSchemaId();
    doReturn(computeRequest).when(request).getComputeRequest();
    doReturn(version.kafkaTopicName()).when(request).getResourceName();
    ComputeRouterRequestKeyV1 key = new ComputeRouterRequestKeyV1(0, ByteBuffer.wrap(keyString.getBytes()), partition);
    ComputeRouterRequestKeyV1 missingKey =
        new ComputeRouterRequestKeyV1(1, ByteBuffer.wrap(missingKeyString.getBytes()), partition);
    doReturn(Arrays.asList(key, missingKey)).when(request).getKeys();

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    ComputeResponseWrapper computeResponse = (ComputeResponseWrapper) argumentCaptor.getValue();
    Assert.assertEquals(computeResponse.isStreamingResponse(), request.isStreamingRequest());
    Assert.assertEquals(computeResponse.getRecordCount(), keySet.size());
    Assert.assertEquals(computeResponse.getMultiChunkLargeValueCount(), 0);
    Assert.assertEquals(computeResponse.getCompressionStrategy(), CompressionStrategy.NO_OP);

    Assert.assertEquals(computeResponse.getDotProductCount(), 1);
    Assert.assertEquals(computeResponse.getHadamardProductCount(), 1);
    Assert.assertEquals(computeResponse.getCountOperatorCount(), 0);
    Assert.assertEquals(computeResponse.getCosineSimilarityCount(), 0);

    Assert.assertEquals(computeResponse.getValueSize(), valueBytes.length);

    int expectedReadComputeOutputSize = 0;
    RecordDeserializer<ComputeResponseRecordV1> responseDeserializer =
        SerializerDeserializerFactory.getAvroSpecificDeserializer(ComputeResponseRecordV1.class);
    for (ComputeResponseRecordV1 record: responseDeserializer
        .deserializeObjects(computeResponse.getResponseBody().array())) {
      if (record.getKeyIndex() < 0) {
        Assert.assertEquals(record.getValue(), StreamingUtils.EMPTY_BYTE_BUFFER);
      } else {
        Assert.assertEquals(record.getKeyIndex(), 0);
        Assert.assertNotEquals(record.getValue(), StreamingUtils.EMPTY_BYTE_BUFFER);
        expectedReadComputeOutputSize += record.getValue().limit();
      }
    }
    Assert.assertEquals(computeResponse.getReadComputeOutputSize(), expectedReadComputeOutputSize);
  }
}
