package com.linkedin.venice.listener;

import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.intThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
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
import com.linkedin.davinci.listener.response.ReplicaIngestionResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.IngestionMetadataRetriever;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.store.StorageEngine;
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
import com.linkedin.venice.guid.JavaUtilGuidV4Generator;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.listener.grpc.handlers.GrpcStorageReadRequestHandler;
import com.linkedin.venice.listener.grpc.handlers.VeniceServerGrpcHandler;
import com.linkedin.venice.listener.request.AdminRequest;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.HeartbeatRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.request.TopicPartitionIngestionContextRequest;
import com.linkedin.venice.listener.response.AbstractReadResponse;
import com.linkedin.venice.listener.response.ComputeResponseWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.MultiKeyResponseWrapper;
import com.linkedin.venice.listener.response.SingleGetResponseWrapper;
import com.linkedin.venice.listener.response.stats.AbstractReadResponseStats;
import com.linkedin.venice.listener.response.stats.MultiKeyResponseStats;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
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
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.mock.SimplePartitioner;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.request.RequestHelper;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.ValueSize;
import com.linkedin.venice.utils.concurrent.BlockingQueueType;
import com.linkedin.venice.utils.concurrent.ThreadPoolFactory;
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
import java.util.function.IntFunction;
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
import org.testng.annotations.DataProvider;
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
  private final int numberOfExecutionThreads = Runtime.getRuntime().availableProcessors();
  private final ThreadPoolExecutor parallelExecutor = ThreadPoolFactory.createThreadPool(
      numberOfExecutionThreads,
      this.getClass().getSimpleName(),
      4096,
      BlockingQueueType.LINKED_BLOCKING_QUEUE);
  private final Store store = mock(Store.class);
  private final Version version = mock(Version.class);
  private final StorageEngine storageEngine = mock(StorageEngine.class);
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
  private final ChunkedValueManifestSerializer chunkedValueManifestSerializer =
      new ChunkedValueManifestSerializer(true);
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
  private final PubSubTopicRepository topicRepository = new PubSubTopicRepository();

  private PubSubContext pubSubContext;

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
    doReturn(true).when(storeRepository).isReadComputationEnabled(any());
    doReturn(new NoopCompressor()).when(compressorFactory).getCompressor(any(), any(), anyInt());

    RocksDBServerConfig rocksDBServerConfig = mock(RocksDBServerConfig.class);
    doReturn(rocksDBServerConfig).when(serverConfig).getRocksDBServerConfig();
    pubSubContext = DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
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

  private enum ParallelQueryProcessing {
    PARALLEL(true), SEQUENTIAL(false);

    final boolean configValue;

    ParallelQueryProcessing(boolean configValue) {
      this.configValue = configValue;
    }
  }

  @DataProvider(name = "storageReadRequestHandlerParams")
  public Object[][] storageReadRequestHandlerParams() {
    int smallRecordCount = numberOfExecutionThreads * 10;
    int largeRecordCount = numberOfExecutionThreads * 100;
    return new Object[][] { { ParallelQueryProcessing.SEQUENTIAL, smallRecordCount, ValueSize.SMALL_VALUE },
        { ParallelQueryProcessing.SEQUENTIAL, smallRecordCount, ValueSize.LARGE_VALUE },
        { ParallelQueryProcessing.SEQUENTIAL, largeRecordCount, ValueSize.SMALL_VALUE },
        { ParallelQueryProcessing.SEQUENTIAL, largeRecordCount, ValueSize.LARGE_VALUE },
        { ParallelQueryProcessing.PARALLEL, smallRecordCount, ValueSize.SMALL_VALUE },
        { ParallelQueryProcessing.PARALLEL, smallRecordCount, ValueSize.LARGE_VALUE },
        { ParallelQueryProcessing.PARALLEL, largeRecordCount, ValueSize.SMALL_VALUE },
        { ParallelQueryProcessing.PARALLEL, largeRecordCount, ValueSize.LARGE_VALUE } };
  }

  private StorageReadRequestHandler createStorageReadRequestHandler() {
    return createStorageReadRequestHandler(false, MultiGetResponseWrapper::new);
  }

  private StorageReadRequestHandler createStorageReadRequestHandler(
      boolean parallelBatchGetEnabled,
      IntFunction<MultiGetResponseWrapper> multiGetResponseProvider) {
    return new StorageReadRequestHandler(
        serverConfig,
        parallelBatchGetEnabled ? parallelExecutor : executor,
        parallelBatchGetEnabled ? parallelExecutor : executor,
        storageEngineRepository,
        storeRepository,
        schemaRepository,
        ingestionMetadataRetriever,
        readMetadataRetriever,
        healthCheckService,
        compressorFactory,
        Optional.empty(),
        multiGetResponseProvider,
        ComputeResponseWrapper::new);
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
    SingleGetResponseWrapper responseObject = (SingleGetResponseWrapper) argumentCaptor.getValue();
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

  @Test(dataProvider = "storageReadRequestHandlerParams")
  public void testParallelMultiGet(ParallelQueryProcessing parallel, int recordCount, ValueSize largeValue)
      throws Exception {

    doReturn(largeValue.config).when(version).isChunkingEnabled();
    StoreVersionState svs = mock(StoreVersionState.class);
    doReturn(largeValue.config).when(svs).getChunked();
    doReturn(svs).when(storageEngine).getStoreVersionState();

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
    int chunkSize = recordCount / numberOfExecutionThreads;
    GUID guid = new JavaUtilGuidV4Generator().getGuid();
    int sequenceNumber = 0;

    // Prepare multiGet records belong to specific sub-partitions, if the router does not have the right logic to figure
    // out
    // the sub-partition, the test will fail. Here we use SimplePartitioner to generate sub-partition id, as it only
    // considers the first byte of the key.
    for (int i = 0; i < recordCount; ++i) {
      MultiGetRouterRequestKeyV1 requestKey = new MultiGetRouterRequestKeyV1();
      String keyString = keyPrefix + i;
      byte[] keyBytes = keySerializer.serialize(null, keyString);
      requestKey.keyBytes = ByteBuffer.wrap(keyBytes);
      requestKey.keyIndex = i;
      requestKey.partitionId = 0;
      String valueString = valuePrefix + i;
      byte[] valueBytes;
      if (largeValue.config) {
        byte[] chunk1 = ValueRecord
            .create(AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(), valueString.substring(0, 3).getBytes())
            .serialize();
        byte[] chunk2 = ValueRecord
            .create(AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion(), valueString.substring(3).getBytes())
            .serialize();
        List<ByteBuffer> keysWithChunkingSuffix = new ArrayList<>(2);
        ByteBuffer chunk1KeyBytes = keyWithChunkingSuffixSerializer
            .serializeChunkedKey(keyBytes, new ChunkedKeySuffix(new ChunkId(guid, 0, sequenceNumber++, 0), true));
        ByteBuffer chunk2KeyBytes = keyWithChunkingSuffixSerializer
            .serializeChunkedKey(keyBytes, new ChunkedKeySuffix(new ChunkId(guid, 0, sequenceNumber++, 1), true));
        keysWithChunkingSuffix.add(chunk1KeyBytes);
        keysWithChunkingSuffix.add(chunk2KeyBytes);
        doReturn(chunk1).when(storageEngine).get(0, chunk1KeyBytes);
        doReturn(chunk2).when(storageEngine).get(0, chunk2KeyBytes);
        ChunkedValueManifest chunkedValueManifest =
            new ChunkedValueManifest(keysWithChunkingSuffix, schemaId, valueString.length());
        valueBytes = chunkedValueManifestSerializer.serialize("", chunkedValueManifest);
      } else {
        valueBytes = valueString.getBytes();
      }
      byte[] valueRecordContainerBytes = ValueRecord.create(
          largeValue.config ? AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion() : schemaId,
          valueBytes).serialize();
      if (largeValue.config) {
        keyBytes = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(keyBytes);
      }
      doReturn(valueRecordContainerBytes).when(storageEngine).get(0, ByteBuffer.wrap(keyBytes));
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

    /**
     * Special {@link com.linkedin.davinci.listener.response.ReadResponseStats} implementation to reliably trigger a
     * race condition. The race can still happen without the sleep (assuming the stats handling code regressed to a
     * buggy state), but it is less likely.
     */
    class MultiKeyResponseStatsWithSlowIncrement extends MultiKeyResponseStats {
      @Override
      public void incrementMultiChunkLargeValueCount() {
        int currentValue = multiChunkLargeValueCount;
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        multiChunkLargeValueCount = currentValue + 1;
      }

      /**
       * Previously, in the buggy code, there was no merge logic, so we simulate this behavior here.
       */
      @Override
      public void merge(ReadResponseStatsRecorder other) {
        if (this == other) {
          return;
        }
        super.merge(other);
      }
    }

    doReturn(parallel.configValue).when(serverConfig).isEnableParallelBatchGet();
    doReturn(chunkSize).when(serverConfig).getParallelBatchGetChunkSize();

    // By using a single shared instance, we simulate the previous structure of the code, and we expect to see a metric
    // underestimation. This is a kind of "test of the test", to help validate that the test is indeed capable of
    // catching the regression (see the related verifications at the end).
    MultiKeyResponseStatsWithSlowIncrement sharedInstance = new MultiKeyResponseStatsWithSlowIncrement();
    IntFunction<MultiGetResponseWrapper> invalidResponseProvider = s -> new MultiGetResponseWrapper(s, sharedInstance);
    IntFunction<MultiGetResponseWrapper> validResponseProvider =
        s -> new MultiGetResponseWrapper(s, new MultiKeyResponseStatsWithSlowIncrement());
    IntFunction<MultiGetResponseWrapper>[] responseProviders =
        new IntFunction[] { invalidResponseProvider, validResponseProvider };

    for (IntFunction<MultiGetResponseWrapper> responseProvider: responseProviders) {
      StorageReadRequestHandler requestHandler =
          createStorageReadRequestHandler(parallel.configValue, responseProvider);
      reset(context);
      long startTime = System.currentTimeMillis();
      requestHandler.channelRead(context, request);
      verify(context, timeout(10000)).writeAndFlush(argumentCaptor.capture());
      long timeSpent = System.currentTimeMillis() - startTime;
      System.out.println("Time spent: " + timeSpent + " ms for " + recordCount + " records.");

      Object response = argumentCaptor.getValue();
      assertTrue(response instanceof AbstractReadResponse, "The response should be castable to AbstractReadResponse.");
      AbstractReadResponse multiGetResponseWrapper = (AbstractReadResponse) response;
      RecordDeserializer<MultiGetResponseRecordV1> deserializer =
          SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);
      byte[] responseBytes = new byte[multiGetResponseWrapper.getResponseBody().readableBytes()];
      multiGetResponseWrapper.getResponseBody().getBytes(0, responseBytes);
      Iterable<MultiGetResponseRecordV1> values = deserializer.deserializeObjects(responseBytes);
      Map<Integer, String> results = new HashMap<>();
      values.forEach(K -> {
        String valueString = new String(K.value.array(), StandardCharsets.UTF_8);
        results.put(K.keyIndex, valueString);
      });
      assertEquals(results.size(), recordCount);
      for (int i = 0; i < recordCount; i++) {
        assertEquals(results.get(i), allValueStrings.get(i));
      }

      ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
      multiGetResponseWrapper.getStatsRecorder().recordMetrics(stats);
      if (largeValue.config) {
        /**
         * The assertion below can catch an issue where metrics are inaccurate during parallel batch gets. This was due
         * to a race condition which is now fixed.
         */
        if (parallel.configValue && responseProvider == invalidResponseProvider) {
          /**
           * With the {@link invalidResponseProvider} and {@link MultiKeyResponseStatsWithSlowIncrement}, we simulate
           * the buggy code, which is vulnerable to the race condition, and so we expect the underestimation.
           */
          verify(stats).recordMultiChunkLargeValueCount(intThat(recordedCount -> recordedCount < recordCount));
        } else {
          /**
           * With only the {@link MultiKeyResponseStatsWithSlowIncrement} in play, the new code should be resilient to
           * the race.
           */
          verify(stats).recordMultiChunkLargeValueCount(recordCount);
        }
      } else {
        verify(stats, never()).recordMultiChunkLargeValueCount(anyInt());
      }
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
    assertTrue(shortcutResponse.getMessage().contains(exceptionMessage));

    // Asserting that the exception got logged
    Assert.assertTrue(errorLogCount.get() > 0);
  }

  @Test
  public void testAdminRequestsPassInStorageExecutionHandler() throws Exception {
    PubSubTopic topic = topicRepository.getTopic("test_store_v1");
    int expectedPartitionId = 12345;

    // [0]""/[1]"action"/[2]"store_version"/[3]"dump_ingestion_state"
    String uri =
        "/" + QueryAction.ADMIN.toString().toLowerCase() + "/" + topic + "/" + ServerAdminAction.DUMP_INGESTION_STATE;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    AdminRequest request = AdminRequest.parseAdminHttpRequest(httpRequest, URI.create(httpRequest.uri()));

    // Mock the AdminResponse from ingestion task
    AdminResponse expectedAdminResponse = new AdminResponse();
    PartitionConsumptionState state = new PartitionConsumptionState(
        new PubSubTopicPartitionImpl(topic, expectedPartitionId),
        new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer(), pubSubContext),
        pubSubContext,
        false,
        Schema.create(Schema.Type.STRING));
    expectedAdminResponse.addPartitionConsumptionState(state);
    doReturn(expectedAdminResponse).when(ingestionMetadataRetriever)
        .getConsumptionSnapshots(eq(topic.getName()), any());

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
    ReplicaIngestionResponse expectedReplicaIngestionResponse = new ReplicaIngestionResponse();
    String jsonStr = "{\n" + "\"kafkaUrl\" : {\n" + "  TP(topic: \"" + topic + "\", partition: " + expectedPartitionId
        + ") : {\n" + "      \"latestOffset\" : 0,\n" + "      \"offsetLag\" : 1,\n" + "      \"msgRate\" : 2.0,\n"
        + "      \"byteRate\" : 6.0,\n" + "      \"consumerIdStr\" : \"consumer1\",\n"
        + "      \"elapsedTimeSinceLastConsumerPollInMs\" : 7,\n"
        + "      \"elapsedTimeSinceLastRecordForPartitionInMs\" : 8,\n"
        + "      \"versionTopicName\" : \"test_store_v1\"\n" + "    }\n" + "  }\n" + "}";
    byte[] expectedTopicPartitionContext = jsonStr.getBytes();
    expectedReplicaIngestionResponse.setPayload(expectedTopicPartitionContext);
    doReturn(expectedReplicaIngestionResponse).when(ingestionMetadataRetriever)
        .getTopicPartitionIngestionContext(eq(topic), eq(topic), eq(expectedPartitionId));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);
    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    ReplicaIngestionResponse replicaIngestionResponse = (ReplicaIngestionResponse) argumentCaptor.getValue();
    String topicPartitionIngestionContextStr = new String(replicaIngestionResponse.getPayload());
    assertTrue(topicPartitionIngestionContextStr.contains(topic));
    assertEquals(replicaIngestionResponse.getPayload(), expectedTopicPartitionContext);
  }

  @Test
  public void testHeartbeatLagRequestsPassInStorageExecutionHandler() throws Exception {
    String topic = "test_store_v1";
    int expectedPartitionId = 12345;
    boolean filterLag = true;
    // [0]""/[1]"action"/[2]"optional topic filter"/[3]"optional partition filter"/[4]"optional lag filter"
    String uri = "/" + QueryAction.HOST_HEARTBEAT_LAG.toString().toLowerCase() + "/" + topic + "/" + expectedPartitionId
        + "/" + filterLag;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    HeartbeatRequest request =
        HeartbeatRequest.parseGetHttpRequest(uri, RequestHelper.getRequestParts(URI.create(httpRequest.uri())));
    System.out.println(request.getTopic() + " " + request.getPartition() + " " + request.isFilterLagReplica());
    // Mock the TopicPartitionIngestionContextResponse from heartbeat service
    ReplicaIngestionResponse expectedReplicaIngestionResponse = new ReplicaIngestionResponse();
    String jsonStr = "{}";
    byte[] expectedTopicPartitionContext = jsonStr.getBytes();
    expectedReplicaIngestionResponse.setPayload(expectedTopicPartitionContext);
    doReturn(expectedReplicaIngestionResponse).when(ingestionMetadataRetriever)
        .getHeartbeatLag(eq(topic), eq(expectedPartitionId), eq(filterLag));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);
    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    ReplicaIngestionResponse replicaIngestionResponse = (ReplicaIngestionResponse) argumentCaptor.getValue();
    assertEquals(replicaIngestionResponse.getPayload(), expectedTopicPartitionContext);
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
    doReturn(2).when(request).getKeyCount();

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    if (!readComputationEnabled) {
      HttpShortcutResponse errorResponse = (HttpShortcutResponse) argumentCaptor.getValue();
      assertEquals(errorResponse.getStatus(), HttpResponseStatus.METHOD_NOT_ALLOWED);
    } else {
      ComputeResponseWrapper computeResponse = (ComputeResponseWrapper) argumentCaptor.getValue();
      assertEquals(computeResponse.isStreamingResponse(), request.isStreamingRequest());
      assertEquals(computeResponse.getCompressionStrategy(), CompressionStrategy.NO_OP);

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
          expectedReadComputeOutputSize += record.getValue().remaining();
        }
      }

      double expectedReadComputeEfficiency = (double) valueBytes.length / (double) expectedReadComputeOutputSize;

      ServerHttpRequestStats stats = mock(ServerHttpRequestStats.class);
      computeResponse.getStatsRecorder().recordMetrics(stats);
      verify(stats).recordDotProductCount(1);
      verify(stats).recordHadamardProduct(1);
      verify(stats).recordReadComputeEfficiency(expectedReadComputeEfficiency);
      verify(stats, never()).recordMultiChunkLargeValueCount(anyInt());
      verify(stats, never()).recordCountOperator(anyInt());
      verify(stats, never()).recordCosineSimilarityCount(anyInt());
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
    SingleGetResponseWrapper responseObject = (SingleGetResponseWrapper) argumentCaptor.getValue();
    assertTrue(responseObject.isFound());
    assertEquals(responseObject.getValueRecord().getDataInBytes(), valueString.getBytes());

    // After that first request, the original storage engine gets closed, and a second storage engine takes its place.
    when(storageEngine.get(anyInt(), any(ByteBuffer.class))).thenThrow(new PersistenceFailureException());
    when(storageEngine.isClosed()).thenReturn(true);
    StorageEngine storageEngine2 = mock(StorageEngine.class);
    when(storageEngine2.get(anyInt(), any(ByteBuffer.class))).thenReturn(valueRecord.serialize());
    doReturn(storageEngine2).when(storageEngineRepository).getLocalStorageEngine(any());

    // Second request should not use the stale storage engine
    requestHandler.channelRead(context, request);
    verify(storageEngine, times(1)).get(anyInt(), any(ByteBuffer.class)); // No extra invocation
    verify(storageEngine2, times(1)).get(anyInt(), any(ByteBuffer.class)); // Good one
    verify(context, times(2)).writeAndFlush(argumentCaptor.capture());
    responseObject = (SingleGetResponseWrapper) argumentCaptor.getValue();
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

    assertEquals(builder.getErrorCode(), VeniceReadResponseStatus.INTERNAL_ERROR.getCode());
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

  @Test
  public void testSingleGetWithKeyNotFound() throws Exception {
    String keyString = "missing-key";
    int partition = 2;
    doReturn(null).when(storageEngine).get(partition, ByteBuffer.wrap(keyString.getBytes()));

    // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
    String uri = "/" + TYPE_STORAGE + "/test-topic_v1/" + partition + "/" + keyString;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    GetRouterRequest request =
        GetRouterRequest.parseGetHttpRequest(httpRequest, RequestHelper.getRequestParts(URI.create(httpRequest.uri())));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, times(1)).writeAndFlush(argumentCaptor.capture());
    SingleGetResponseWrapper responseObject = (SingleGetResponseWrapper) argumentCaptor.getValue();
    Assert.assertNull(responseObject.getValueRecord());
    assertEquals(((AbstractReadResponseStats) responseObject.getStats()).getKeyNotFoundCount(), 1);
  }

  @Test
  public void testMultiGetWithKeyNotFound() throws Exception {
    int recordCount = 10;
    int missingRecordCount = 3;
    RecordSerializer<MultiGetRouterRequestKeyV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);
    List<MultiGetRouterRequestKeyV1> keys = new ArrayList<>();
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);

    for (int i = 0; i < recordCount; ++i) {
      MultiGetRouterRequestKeyV1 requestKey = new MultiGetRouterRequestKeyV1();
      String keyString = "key_" + i;
      byte[] keyBytes = keySerializer.serialize(null, keyString);
      requestKey.keyBytes = ByteBuffer.wrap(keyBytes);
      requestKey.keyIndex = i;
      requestKey.partitionId = 0;

      if (i < (recordCount - missingRecordCount)) {
        String valueString = "value_" + i;
        byte[] valueBytes = ValueRecord.create(1, valueString.getBytes()).serialize();
        doReturn(valueBytes).when(storageEngine).get(eq(0), eq(requestKey.keyBytes));
      } else {
        doReturn(null).when(storageEngine).get(eq(0), eq(requestKey.keyBytes));
      }
      keys.add(requestKey);
    }

    String uri = "/" + TYPE_STORAGE + "/test-topic_v1";
    FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    httpRequest.headers()
        .set(
            HttpConstants.VENICE_API_VERSION,
            ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion());
    httpRequest.content().writeBytes(serializer.serializeObjects(keys));
    MultiGetRouterRequestWrapper request = MultiGetRouterRequestWrapper
        .parseMultiGetHttpRequest(httpRequest, RequestHelper.getRequestParts(URI.create(uri)));

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, timeout(1000).times(1)).writeAndFlush(argumentCaptor.capture());
    MultiKeyResponseWrapper responseObject = (MultiKeyResponseWrapper) argumentCaptor.getValue();
    assertEquals(((AbstractReadResponseStats) responseObject.getStats()).getKeyNotFoundCount(), missingRecordCount);
  }

  @Test
  public void testComputeWithKeyNotFound() throws Exception {
    int recordCount = 2;
    int missingRecordCount = 1;

    String valueSchemaStr = "{" + "  \"type\": \"record\"," + "  \"name\": \"User\"," + "  \"fields\": ["
        + "    {\"name\": \"name\", \"type\": \"string\"}" + "  ]" + "}";
    Schema valueSchema = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(valueSchemaStr);
    SchemaEntry valueSchemaEntry = new SchemaEntry(1, valueSchema);
    doReturn(valueSchemaEntry).when(schemaRepository).getValueSchema(any(), anyInt());

    RecordSerializer<GenericRecord> valueSerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(valueSchema);

    String key1Str = "key_1";
    ByteBuffer key1Bytes = ByteBuffer.wrap(key1Str.getBytes());
    ComputeRouterRequestKeyV1 requestKey1 = new ComputeRouterRequestKeyV1(0, key1Bytes, 0);

    String key2Str = "key_2";
    ByteBuffer key2Bytes = ByteBuffer.wrap(key2Str.getBytes());
    ComputeRouterRequestKeyV1 requestKey2 = new ComputeRouterRequestKeyV1(1, key2Bytes, 0);

    List<ComputeRouterRequestKeyV1> keys = Arrays.asList(requestKey1, requestKey2);

    GenericRecord value = new GenericData.Record(valueSchema);
    value.put("name", "name_1");
    byte[] valueBytes = ValueRecord.create(1, valueSerializer.serialize(value)).serialize();

    byte[] key1BytesArray = key1Str.getBytes();

    doAnswer(invocation -> {
      byte[] bytes = invocation.getArgument(1);
      if (Arrays.equals(bytes, key1BytesArray)) {
        return valueBytes;
      }
      return null;
    }).when(storageEngine).get(eq(0), any(byte[].class));

    doAnswer(invocation -> {
      byte[] bytes = invocation.getArgument(1);
      if (Arrays.equals(bytes, key1BytesArray)) {
        return ByteBuffer.wrap(valueBytes);
      }
      return null;
    }).when(storageEngine).get(eq(0), any(byte[].class), any(ByteBuffer.class));

    doAnswer(invocation -> {
      ByteBuffer bb = invocation.getArgument(1);
      if (bb != null && bb.equals(key1Bytes)) {
        return valueBytes;
      }
      return null;
    }).when(storageEngine).get(eq(0), any(ByteBuffer.class));

    ComputeRequest computeRequest = new ComputeRequest();
    computeRequest.setOperations(Collections.emptyList());
    computeRequest.setResultSchemaStr(new org.apache.avro.util.Utf8(valueSchemaStr));

    ComputeRouterRequestWrapper request = mock(ComputeRouterRequestWrapper.class);
    doReturn(keys).when(request).getKeys();
    doReturn(recordCount).when(request).getKeyCount();
    doReturn("test-store_v1").when(request).getResourceName();
    doReturn("test-store").when(request).getStoreName();
    doReturn(RequestType.COMPUTE).when(request).getRequestType();
    doReturn(computeRequest).when(request).getComputeRequest();
    doReturn(1).when(request).getValueSchemaId();
    doReturn(false).when(request).shouldRequestBeTerminatedEarly();
    doReturn(false).when(request).isStreamingRequest();

    StorageReadRequestHandler requestHandler = createStorageReadRequestHandler();
    requestHandler.channelRead(context, request);

    verify(context, timeout(1000).times(1)).writeAndFlush(argumentCaptor.capture());
    MultiKeyResponseWrapper responseObject = (MultiKeyResponseWrapper) argumentCaptor.getValue();
    assertEquals(((AbstractReadResponseStats) responseObject.getStats()).getKeyNotFoundCount(), missingRecordCount);
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
