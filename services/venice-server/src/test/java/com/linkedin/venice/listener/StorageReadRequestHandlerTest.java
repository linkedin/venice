package com.linkedin.venice.listener;

import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.PartitionConsumptionState;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.IngestionMetadataRetriever;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
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
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.compute.protocol.request.ComputeRequest;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.PersistenceFailureException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.grpc.GrpcErrorCodes;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.grpc.handlers.GrpcStorageReadRequestHandler;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler;
import com.linkedin.venice.listener.request.AdminRequest;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.request.TopicPartitionIngestionContextRequest;
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
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.request.RequestHelper;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
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
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.net.URI;
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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.mockito.ArgumentCaptor;
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
  private final IngestionMetadataRetriever ingestionMetadataRetriever = mock(IngestionMetadataRetriever.class);
  private final ReadMetadataRetriever readMetadataRetriever = mock(ReadMetadataRetriever.class);
  private final VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
  private final VenicePartitioner partitioner = new SimplePartitioner();

  @BeforeMethod
  public void setUp() {
    doReturn(store).when(storeRepository).getStoreOrThrow(any());
    doReturn(version).when(store).getVersion(anyInt());
    doReturn(version).when(store).getVersionOrThrow(anyInt());

    doReturn("test-store_v1").when(version).kafkaTopicName();
    PartitionerConfig partitionerConfig =
        new PartitionerConfigImpl(partitioner.getClass().getName(), Collections.emptyMap(), 1);
    doReturn(partitionerConfig).when(version).getPartitionerConfig();

    doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(any());
    doReturn(new NoopCompressor()).when(compressorFactory).getCompressor(any(), any());

    RocksDBServerConfig rocksDBServerConfig = mock(RocksDBServerConfig.class);
    doReturn(rocksDBServerConfig).when(serverConfig).getRocksDBServerConfig();
  }

  @AfterMethod
  public void cleanUp() {
    reset(
        storageEngine,
        storageEngineRepository,
        storeRepository,
        schemaRepository,
        compressorFactory,
        healthCheckService,
        ingestionMetadataRetriever,
        readMetadataRetriever,
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
        ingestionMetadataRetriever,
        readMetadataRetriever,
        healthCheckService,
        false,
        parallelBatchGetEnabled,
        parallelBatchGetChunkSize,
        serverConfig,
        compressorFactory,
        Optional.empty());
  }

  @Test
  public void storageExecutionHandlerPassesRequestsAndGeneratesResponses() throws Exception {
    String keyString = "test-key";
    String valueString = "test-value";
    int schemaId = 1;
    int partition = 2;
    byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
    doReturn(valueBytes).when(storageEngine).get(partition, ByteBuffer.wrap(keyString.getBytes()));

    // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
    String uri = "/" + TYPE_STORAGE + "/test-topic_v1/" + partition + "/" + keyString;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    GetRouterRequest request =
        GetRouterRequest.parseGetHttpRequest(httpRequest, RequestHelper.getRequestParts(URI.create(httpRequest.uri())));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    StorageResponseObject responseObject = (StorageResponseObject) argumentCaptor.getValue();
    assertEquals(responseObject.getValueRecord().getDataInBytes(), valueString.getBytes());
    assertEquals(responseObject.getValueRecord().getSchemaId(), schemaId);
  }

  @Test
  public void testDiskHealthCheckService() throws Exception {
    doReturn(true).when(healthCheckService).isDiskHealthy();

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, new HealthCheckRequest());

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    HttpShortcutResponse healthCheckResponse = (HttpShortcutResponse) argumentCaptor.getValue();
    assertEquals(healthCheckResponse.getStatus(), HttpResponseStatus.OK);
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
      requestKey.keyBytes = ByteBuffer.wrap(keyBytes);
      requestKey.keyIndex = i;
      requestKey.partitionId = 0;
      String valueString = valuePrefix + i;
      byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
      doReturn(valueBytes).when(storageEngine).get(0, ByteBuffer.wrap(keyBytes));
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
    MultiGetRouterRequestWrapper request = MultiGetRouterRequestWrapper
        .parseMultiGetHttpRequest(httpRequest, RequestHelper.getRequestParts(URI.create(httpRequest.uri())));

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
    assertEquals(results.size(), recordCount);
    for (int i = 0; i < recordCount; i++) {
      assertEquals(results.get(i), allValueStrings.get(i));
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
    GetRouterRequest request =
        GetRouterRequest.parseGetHttpRequest(httpRequest, RequestHelper.getRequestParts(URI.create(httpRequest.uri())));

    byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
    doReturn(valueBytes).when(storageEngine).get(partition, ByteBuffer.wrap(keyString.getBytes()));

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
    assertEquals(shortcutResponse.getStatus(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
    assertEquals(shortcutResponse.getMessage(), exceptionMessage);

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
    AdminRequest request = AdminRequest.parseAdminHttpRequest(httpRequest, URI.create(httpRequest.uri()));

    // Mock the AdminResponse from ingestion task
    AdminResponse expectedAdminResponse = new AdminResponse();
    PartitionConsumptionState state = new PartitionConsumptionState(
        Utils.getReplicaId(topic, expectedPartitionId),
        expectedPartitionId,
        new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer()),
        false);
    expectedAdminResponse.addPartitionConsumptionState(state);
    doReturn(expectedAdminResponse).when(ingestionMetadataRetriever).getConsumptionSnapshots(eq(topic), any());

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    AdminResponse adminResponse = (AdminResponse) argumentCaptor.getValue();
    assertEquals(adminResponse.getResponseRecord().partitionConsumptionStates.size(), 1);
    assertEquals(adminResponse.getResponseRecord().partitionConsumptionStates.get(0).partitionId, expectedPartitionId);
    assertEquals(
        adminResponse.getResponseSchemaIdHeader(),
        AvroProtocolDefinition.SERVER_ADMIN_RESPONSE.getCurrentProtocolVersion());
  }

  @Test
  public void testTopicPartitionIngestionContextRequestsPassInStorageExecutionHandler() throws Exception {
    String topic = "test_store_v1";
    int expectedPartitionId = 12345;
    // [0]""/[1]"action"/[2]"version topic"/[3]"topic name"/[4]"partition number"
    String uri = "/" + QueryAction.TOPIC_PARTITION_INGESTION_CONTEXT.toString().toLowerCase() + "/" + topic + "/"
        + topic + "/" + expectedPartitionId;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    TopicPartitionIngestionContextRequest request = TopicPartitionIngestionContextRequest
        .parseGetHttpRequest(uri, RequestHelper.getRequestParts(URI.create(httpRequest.uri())));

    // Mock the TopicPartitionIngestionContextResponse from ingestion task
    TopicPartitionIngestionContextResponse expectedTopicPartitionIngestionContextResponse =
        new TopicPartitionIngestionContextResponse();
    String jsonStr = "{\n" + "\"kafkaUrl\" : {\n" + "  TP(topic: \"" + topic + "\", partition: " + expectedPartitionId
        + ") : {\n" + "      \"latestOffset\" : 0,\n" + "      \"offsetLag\" : 1,\n" + "      \"msgRate\" : 2.0,\n"
        + "      \"byteRate\" : 4.0,\n" + "      \"consumerIdx\" : 6,\n"
        + "      \"elapsedTimeSinceLastPollInMs\" : 7\n" + "    }\n" + "  }\n" + "}";
    byte[] expectedTopicPartitionContext = jsonStr.getBytes();
    expectedTopicPartitionIngestionContextResponse.setTopicPartitionIngestionContext(expectedTopicPartitionContext);
    doReturn(expectedTopicPartitionIngestionContextResponse).when(ingestionMetadataRetriever)
        .getTopicPartitionIngestionContext(eq(topic), eq(topic), eq(expectedPartitionId));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);
    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    TopicPartitionIngestionContextResponse topicPartitionIngestionContextResponse =
        (TopicPartitionIngestionContextResponse) argumentCaptor.getValue();
    String topicPartitionIngestionContextStr =
        new String(topicPartitionIngestionContextResponse.getTopicPartitionIngestionContext());
    assertTrue(topicPartitionIngestionContextStr.contains(topic));
    assertEquals(
        topicPartitionIngestionContextResponse.getTopicPartitionIngestionContext(),
        expectedTopicPartitionContext);
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
    MetadataFetchRequest testRequest =
        MetadataFetchRequest.parseGetHttpRequest(uri, RequestHelper.getRequestParts(URI.create(httpRequest.uri())));

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
    doReturn(expectedMetadataResponse).when(readMetadataRetriever).getMetadata(eq(storeName));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, testRequest);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    MetadataResponse metadataResponse = (MetadataResponse) argumentCaptor.getValue();
    assertEquals(metadataResponse.getResponseRecord().getVersionMetadata(), versionProperties);
    assertEquals(metadataResponse.getResponseRecord().getKeySchema(), keySchema);
    assertEquals(metadataResponse.getResponseRecord().getValueSchemas(), valueSchemas);
  }

  @Test
  public void testUnrecognizedRequestInStorageExecutionHandler() throws Exception {
    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, new Object());

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    HttpShortcutResponse shortcutResponse = (HttpShortcutResponse) argumentCaptor.getValue();
    assertEquals(shortcutResponse.getStatus(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
    assertEquals(shortcutResponse.getMessage(), "Unrecognized object in StorageExecutionHandler");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHandleComputeRequest(boolean readComputationEnabled) throws Exception {
    doReturn(readComputationEnabled).when(storeRepository).isReadComputationEnabled(any());

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
    AvroSerializer valueSerializer = new AvroSerializer<>(valueRecord.getSchema());
    byte[] valueBytes = ValueRecord.create(schemaEntry.getId(), valueSerializer.serialize(valueRecord)).serialize();
    doReturn(ByteBuffer.wrap(valueBytes)).when(storageEngine).get(eq(partition), eq(keyString.getBytes()), any());

    Set<Object> keySet = new HashSet<>(Arrays.asList(keyString, missingKeyString));
    AvroGenericReadComputeStoreClient storeClient = mock(AvroGenericReadComputeStoreClient.class);
    doReturn("test-store").when(storeClient).getStoreName();
    Schema keySchema = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation("\"string\"");
    new AvroComputeRequestBuilderV3<>(storeClient, getMockSchemaReader(keySchema, valueRecord.getSchema()))
        .dotProduct("listField", Collections.singletonList(4.0f), "dotProduct")
        .hadamardProduct("listField", Collections.singletonList(5.0f), "hadamardProduct")
        .execute(keySet);
    ArgumentCaptor<ComputeRequestWrapper> requestCaptor = ArgumentCaptor.forClass(ComputeRequestWrapper.class);
    verify(storeClient, times(1)).compute(requestCaptor.capture(), any(), any(), any(), anyLong());
    ComputeRequestWrapper computeRequestWrapper = requestCaptor.getValue();
    ComputeRequest computeRequest = ComputeUtils.deserializeComputeRequest(
        OptimizedBinaryDecoderFactory.defaultFactory()
            .createOptimizedBinaryDecoder(ByteBuffer.wrap(computeRequestWrapper.serialize())),
        null);

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
    if (!readComputationEnabled) {
      HttpShortcutResponse errorResponse = (HttpShortcutResponse) argumentCaptor.getValue();
      assertEquals(errorResponse.getStatus(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    } else {
      ComputeResponseWrapper computeResponse = (ComputeResponseWrapper) argumentCaptor.getValue();
      assertEquals(computeResponse.isStreamingResponse(), request.isStreamingRequest());
      assertEquals(computeResponse.getRecordCount(), keySet.size());
      assertEquals(computeResponse.getMultiChunkLargeValueCount(), 0);
      assertEquals(computeResponse.getCompressionStrategy(), CompressionStrategy.NO_OP);

      assertEquals(computeResponse.getDotProductCount(), 1);
      assertEquals(computeResponse.getHadamardProductCount(), 1);
      assertEquals(computeResponse.getCountOperatorCount(), 0);
      assertEquals(computeResponse.getCosineSimilarityCount(), 0);

      assertEquals(computeResponse.getValueSize(), valueBytes.length);

      int expectedReadComputeOutputSize = 0;
      RecordDeserializer<ComputeResponseRecordV1> responseDeserializer =
          SerializerDeserializerFactory.getAvroSpecificDeserializer(ComputeResponseRecordV1.class);
      for (ComputeResponseRecordV1 record: responseDeserializer
          .deserializeObjects(computeResponse.getResponseBody().array())) {
        if (record.getKeyIndex() < 0) {
          assertEquals(record.getValue(), StreamingUtils.EMPTY_BYTE_BUFFER);
        } else {
          assertEquals(record.getKeyIndex(), 0);
          Assert.assertNotEquals(record.getValue(), StreamingUtils.EMPTY_BYTE_BUFFER);
          expectedReadComputeOutputSize += record.getValue().limit();
        }
      }
      assertEquals(computeResponse.getReadComputeOutputSize(), expectedReadComputeOutputSize);
    }
  }

  /**
   * There was a regression where the "perStoreVersionStateMap" inside {@link StorageReadRequestHandler} could be stale
   * during rebalance. In the following rebalance scenario, the storage engine reference in the map would be stale:
   *
   * 1. There is only one assigned partition for this resource, and let us say the storage engine instance is 'se1'.
   * 2. When Helix rebalance decides to move this partition to another node, there is no partition left, so this storage
   *    engine: 'se1' will be closed and removed from the storage engine factory.
   * 3. Later, Helix decides to move back another partition to this node, Storage engine factory will create a new
   *    instance of storage engine: 'se2'.
   * 4. This particular node will bootstrap the rebalanced partition in 'se2'.
   * 5. When the bootstrapping is done, Router will start sending request to the rebalanced partition, but
   *    'StorageReadRequestHandler' is referring to a stale storage engine instance: 'se1', which is empty.
   * 6. The read request to this rebalanced partition will be rejected even this particular node has the partition on
   *    disk and Helix EV/CV.
   */
  @Test
  public void testMissingStorageEngine() throws Exception {
    String keyString = "test-key";
    String valueString = "test-value";
    int schemaVersion = 1;
    ValueRecord valueRecord = ValueRecord.create(schemaVersion, valueString.getBytes());

    when(storageEngine.get(anyInt(), any(ByteBuffer.class))).thenReturn(valueRecord.serialize());

    GetRouterRequest request = mock(GetRouterRequest.class);
    doReturn(SINGLE_GET).when(request).getRequestType();
    doReturn(version.kafkaTopicName()).when(request).getResourceName();
    doReturn(keyString.getBytes()).when(request).getKeyBytes();

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();

    /** This first request will prime the "perStoreVersionStateMap" inside {@link StorageReadRequestHandler} */
    requestHandler.channelRead(context, request);
    verify(storageEngine, times(1)).get(anyInt(), any(ByteBuffer.class));
    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    StorageResponseObject responseObject = (StorageResponseObject) argumentCaptor.getValue();
    assertTrue(responseObject.isFound());
    assertEquals(responseObject.getValueRecord().getDataInBytes(), valueString.getBytes());

    // After that first request, the original storage engine gets closed, and a second storage engine takes its place.
    when(storageEngine.get(anyInt(), any(ByteBuffer.class))).thenThrow(new PersistenceFailureException());
    when(storageEngine.isClosed()).thenReturn(true);
    AbstractStorageEngine storageEngine2 = mock(AbstractStorageEngine.class);
    when(storageEngine2.get(anyInt(), any(ByteBuffer.class))).thenReturn(valueRecord.serialize());
    doReturn(storageEngine2).when(storageEngineRepository).getLocalStorageEngine(any());

    // Second request should not use the stale storage engine
    requestHandler.channelRead(context, request);
    verify(storageEngine, times(1)).get(anyInt(), any(ByteBuffer.class)); // No extra invocation
    verify(storageEngine2, times(1)).get(anyInt(), any(ByteBuffer.class)); // Good one
    verify(context, times(2)).writeAndFlush(argumentCaptor.capture());
    responseObject = (StorageResponseObject) argumentCaptor.getValue();
    assertTrue(responseObject.isFound());
    assertEquals(responseObject.getValueRecord().getDataInBytes(), valueString.getBytes());
  }

  @Test
  public void testGrpcReadReturnsInternalErrorWhenRouterRequestIsNull() {
    VeniceClientRequest clientRequest =
        VeniceClientRequest.newBuilder().setIsBatchRequest(true).setResourceName("testStore_v1").build();
    VeniceServerResponse.Builder builder = VeniceServerResponse.newBuilder();
    GrpcRequestContext ctx = new GrpcRequestContext(clientRequest, builder, null);
    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    GrpcStorageReadRequestHandler grpcReadRequestHandler = spy(new GrpcStorageReadRequestHandler(requestHandler));
    VeniceServerGrpcHandler mockNextHandler = mock(VeniceServerGrpcHandler.class);
    grpcReadRequestHandler.addNextHandler(mockNextHandler);
    doNothing().when(mockNextHandler).processRequest(any());
    grpcReadRequestHandler.processRequest(ctx); // will cause np exception

    assertEquals(builder.getErrorCode(), GrpcErrorCodes.INTERNAL_ERROR);
    assertTrue(builder.getErrorMessage().contains("Internal Error"));
  }

  @Test
  public void testMisRoutedStoreVersion() throws Exception {
    String storeName = "testStore";
    // The request should throw NPE when the handler is trying to handleSingleGetRequest due to null partition and key.
    RouterRequest request = mock(GetRouterRequest.class);
    doReturn(false).when(request).shouldRequestBeTerminatedEarly();
    doReturn(SINGLE_GET).when(request).getRequestType();
    doReturn(Version.composeKafkaTopic(storeName, 1)).when(request).getResourceName();
    doReturn(storeName).when(request).getStoreName();
    Store store = mock(Store.class);
    doReturn(null).when(store).getVersion(anyInt());
    doReturn(store).when(storeRepository).getStore(storeName);
    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);
    ArgumentCaptor<HttpShortcutResponse> shortcutResponseArgumentCaptor =
        ArgumentCaptor.forClass(HttpShortcutResponse.class);
    verify(context).writeAndFlush(shortcutResponseArgumentCaptor.capture());
    Assert.assertTrue(shortcutResponseArgumentCaptor.getValue().isMisroutedStoreVersion());
  }

  @Test
  public void testNoStorageEngineReturn503() throws Exception {
    String storeName = "testStore";
    String keyString = "key_byte";
    GetRouterRequest request = mock(GetRouterRequest.class);
    doReturn(false).when(request).shouldRequestBeTerminatedEarly();
    doReturn(SINGLE_GET).when(request).getRequestType();
    doReturn(Version.composeKafkaTopic(storeName, 1)).when(request).getResourceName();
    doReturn(storeName).when(request).getStoreName();
    Store store = mock(Store.class);
    doReturn(null).when(store).getVersion(anyInt());
    doReturn(store).when(storeRepository).getStore(storeName);
    doReturn(1).when(store).getCurrentVersion();
    when(storageEngine.isClosed()).thenReturn(true);
    doReturn(null).when(storageEngineRepository).getLocalStorageEngine(any());
    doReturn(keyString.getBytes()).when(request).getKeyBytes();
    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);
    ArgumentCaptor<HttpShortcutResponse> shortcutResponseArgumentCaptor =
        ArgumentCaptor.forClass(HttpShortcutResponse.class);
    verify(context).writeAndFlush(shortcutResponseArgumentCaptor.capture());
    Assert.assertEquals(shortcutResponseArgumentCaptor.getValue().getStatus(), SERVICE_UNAVAILABLE);

    // when current version different from resource, return 400
    doReturn(10).when(store).getCurrentVersion();
    requestHandler.channelRead(context, request);
    verify(context, times(2)).writeAndFlush(shortcutResponseArgumentCaptor.capture());

    Assert.assertEquals(shortcutResponseArgumentCaptor.getValue().getStatus(), BAD_REQUEST);
  }

  private SchemaReader getMockSchemaReader(Schema keySchema, Schema valueSchema) {
    SchemaReader schemaReader = mock(SchemaReader.class);
    doReturn(keySchema).when(schemaReader).getKeySchema();
    doReturn(valueSchema).when(schemaReader).getValueSchema(1);
    doReturn(valueSchema).when(schemaReader).getLatestValueSchema();
    doReturn(1).when(schemaReader).getLatestValueSchemaId();
    doReturn(1).when(schemaReader).getValueSchemaId(valueSchema);
    return schemaReader;
  }
}
