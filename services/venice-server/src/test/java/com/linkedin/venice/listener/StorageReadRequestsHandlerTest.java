package com.linkedin.venice.listener;

import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.AdminRequest;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
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
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.unit.kafka.SimplePartitioner;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StorageReadRequestsHandlerTest {
  @Test
  public static void storageExecutionHandlerPassesRequestsAndGeneratesResponses() throws Exception {
    String topic = "temp-test-topic_v1";
    String keyString = "testkey";
    String valueString = "testvalue";
    int schemaId = 1;
    int partition = 3;
    List<Object> outputArray = new ArrayList<Object>();
    byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
    // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
    String uri = "/" + TYPE_STORAGE + "/" + topic + "/" + partition + "/" + keyString;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    GetRouterRequest testRequest = GetRouterRequest.parseGetHttpRequest(httpRequest);

    AbstractStorageEngine storageEngine = mock(AbstractStorageEngine.class);
    doReturn(valueBytes).when(storageEngine).get(partition, ByteBuffer.wrap(keyString.getBytes()));

    StorageEngineRepository testRepository = mock(StorageEngineRepository.class);
    doReturn(storageEngine).when(testRepository).getLocalStorageEngine(topic);

    MetadataRetriever mockMetadataRetriever = mock(MetadataRetriever.class);

    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    RocksDBServerConfig dbServerConfig = mock(RocksDBServerConfig.class);
    doReturn(dbServerConfig).when(serverConfig).getRocksDBServerConfig();

    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    Version version = mock(Version.class);
    PartitionerConfig partitionerConfig = mock(PartitionerConfig.class);
    when(partitionerConfig.getAmplificationFactor()).thenReturn(1);
    when(version.getPartitionerConfig()).thenReturn(partitionerConfig);
    when(store.getVersion(anyInt())).thenReturn(Optional.of(version));
    when(metadataRepo.getStoreOrThrow(anyString())).thenReturn(store);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2));

    try {
      StorageReadRequestsHandler testHandler = new StorageReadRequestsHandler(
          threadPoolExecutor,
          threadPoolExecutor,
          testRepository,
          metadataRepo,
          schemaRepo,
          mockMetadataRetriever,
          null,
          false,
          false,
          10,
          serverConfig,
          mock(StorageEngineBackedCompressorFactory.class),
          Optional.empty());
      testHandler.channelRead(mockCtx, testRequest);

      waitUntilStorageExecutionHandlerRespond(outputArray);

      // parsing of response
      Assert.assertEquals(outputArray.size(), 1);
      StorageResponseObject obj = (StorageResponseObject) outputArray.get(0);
      byte[] response = obj.getValueRecord().getDataInBytes();

      // Verification
      Assert.assertEquals(response, valueString.getBytes());
      Assert.assertEquals(obj.getValueRecord().getSchemaId(), schemaId);
    } finally {
      TestUtils.shutdownExecutor(threadPoolExecutor);
    }
  }

  @Test
  public void testDiskHealthCheckService() throws Exception {
    DiskHealthCheckService healthCheckService = mock(DiskHealthCheckService.class);
    doReturn(true).when(healthCheckService).isDiskHealthy();

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2));
    try {
      StorageEngineRepository testRepository = mock(StorageEngineRepository.class);
      ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
      ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
      MetadataRetriever mockMetadataRetriever = mock(MetadataRetriever.class);
      VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
      RocksDBServerConfig dbServerConfig = mock(RocksDBServerConfig.class);
      doReturn(dbServerConfig).when(serverConfig).getRocksDBServerConfig();

      StorageReadRequestsHandler testHandler = new StorageReadRequestsHandler(
          threadPoolExecutor,
          threadPoolExecutor,
          testRepository,
          metadataRepo,
          schemaRepo,
          mockMetadataRetriever,
          healthCheckService,
          false,
          false,
          10,
          serverConfig,
          mock(StorageEngineBackedCompressorFactory.class),
          Optional.empty());

      ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
      doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
      List<Object> outputs = new ArrayList<Object>();
      when(mockCtx.writeAndFlush(any())).then(i -> {
        outputs.add(i.getArguments()[0]);
        return null;
      });
      HealthCheckRequest healthCheckRequest = new HealthCheckRequest();

      testHandler.channelRead(mockCtx, healthCheckRequest);
      waitUntilStorageExecutionHandlerRespond(outputs);

      Assert.assertTrue(outputs.get(0) instanceof HttpShortcutResponse);
      HttpShortcutResponse healthCheckResponse = (HttpShortcutResponse) outputs.get(0);
      Assert.assertEquals(healthCheckResponse.getStatus(), HttpResponseStatus.OK);
    } finally {
      TestUtils.shutdownExecutor(threadPoolExecutor);
    }
  }

  private static void waitUntilStorageExecutionHandlerRespond(List<Object> outputs) throws Exception {
    // Wait for async stuff to finish
    int count = 1;
    while (outputs.size() < 1) {
      Thread.sleep(10); // on my machine, consistently fails with only 10ms, intermittent at 15ms, success at 20ms
      count += 1;
      if (count > 200) { // two seconds
        throw new RuntimeException("Timeout waiting for StorageExecutionHandler output to appear");
      }
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public static void testMultiGetNotUsingKeyBytes(Boolean isParallel) throws Exception {
    String topic = "temp-test-topic_v1";
    int schemaId = 1;
    List<Object> outputArray = new ArrayList<>();

    // [0]""/[1]"storage"/[2]{$resourceName}
    String uri = "/" + TYPE_STORAGE + "/" + topic;

    RecordSerializer<MultiGetRouterRequestKeyV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);
    List<MultiGetRouterRequestKeyV1> keys = new ArrayList<>();
    String stringSchema = "\"string\"";
    VeniceKafkaSerializer keySerializer = new VeniceAvroKafkaSerializer(stringSchema);
    String keyPrefix = "key_";
    String valuePrefix = "value_";

    SimplePartitioner simplePartitioner = new SimplePartitioner();
    AbstractStorageEngine testStore = mock(AbstractStorageEngine.class);
    Map<Integer, String> allValueStrings = new HashMap<>();
    int recordCount = 10;

    // Prepare multiGet records belong to specific sub-partitions, if the router does not have right logic to figure out
    // the sub-partition, the test will fail. Here we use SimplePartitioner to generate sub-partition id, as it only
    // consider the first byte of the key.
    for (int i = 0; i < recordCount; ++i) {
      MultiGetRouterRequestKeyV1 requestKey = new MultiGetRouterRequestKeyV1();
      byte[] keyBytes = keySerializer.serialize(null, keyPrefix + i);
      int subPartition = simplePartitioner.getPartitionId(keyBytes, 3);
      requestKey.keyBytes = ByteBuffer.wrap(keyBytes);
      requestKey.keyIndex = i;
      requestKey.partitionId = 0;
      String valueString = valuePrefix + i;
      byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
      doReturn(valueBytes).when(testStore).get(subPartition, ByteBuffer.wrap(keyBytes));
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
    MultiGetRouterRequestWrapper testRequest = MultiGetRouterRequestWrapper.parseMultiGetHttpRequest(httpRequest);

    StorageEngineRepository testRepository = mock(StorageEngineRepository.class);
    doReturn(testStore).when(testRepository).getLocalStorageEngine(topic);

    MetadataRetriever mockMetadataRetriever = mock(MetadataRetriever.class);

    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    RocksDBServerConfig dbServerConfig = mock(RocksDBServerConfig.class);
    doReturn(dbServerConfig).when(serverConfig).getRocksDBServerConfig();

    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    Version version = mock(Version.class);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setPartitionerClass(SimplePartitioner.class.getName());
    partitionerConfig.setAmplificationFactor(3);
    when(version.getPartitionerConfig()).thenReturn(partitionerConfig);
    when(store.getVersion(anyInt())).thenReturn(Optional.of(version));
    when(metadataRepo.getStoreOrThrow(anyString())).thenReturn(store);

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2));

    try {
      StorageReadRequestsHandler testHandler = new StorageReadRequestsHandler(
          threadPoolExecutor,
          threadPoolExecutor,
          testRepository,
          metadataRepo,
          schemaRepo,
          mockMetadataRetriever,
          null,
          false,
          isParallel,
          10,
          serverConfig,
          mock(StorageEngineBackedCompressorFactory.class),
          Optional.empty());
      testHandler.channelRead(mockCtx, testRequest);
      waitUntilStorageExecutionHandlerRespond(outputArray);

      // parsing of response
      Assert.assertEquals(outputArray.size(), 1);
      Assert.assertTrue(outputArray.get(0) instanceof MultiGetResponseWrapper);
      MultiGetResponseWrapper multiGetResponseWrapper = (MultiGetResponseWrapper) outputArray.get(0);
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
    } finally {
      TestUtils.shutdownExecutor(threadPoolExecutor);
    }
  }

  @Test
  public static void storageExecutionHandlerLogsExceptions() throws Exception {
    String topic = "temp-test-topic_v1";
    String keyString = "testkey";
    String valueString = "testvalue";
    int schemaId = 1;
    int partition = 3;
    long expectedOffset = 12345L;
    List<Object> outputArray = new ArrayList<Object>();
    byte[] valueBytes = ValueRecord.create(schemaId, valueString.getBytes()).serialize();
    // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
    String uri = "/" + TYPE_STORAGE + "/" + topic + "/" + partition + "/" + keyString;
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    GetRouterRequest testRequest = GetRouterRequest.parseGetHttpRequest(httpRequest);

    AbstractStorageEngine testStore = mock(AbstractStorageEngine.class);
    doReturn(valueBytes).when(testStore).get(partition, ByteBuffer.wrap(keyString.getBytes()));

    StorageEngineRepository testRepository = mock(StorageEngineRepository.class);

    final String exceptionMessage = "Exception thrown in Mock";
    // Forcing an exception to be thrown.
    doThrow(new VeniceException(exceptionMessage)).when(testRepository).getLocalStorageEngine(topic);

    MetadataRetriever mockMetadataRetriever = mock(MetadataRetriever.class);

    ReadOnlySchemaRepository schemaRepo = mock(ReadOnlySchemaRepository.class);
    ReadOnlyStoreRepository metadataRepo = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    Version version = mock(Version.class);
    PartitionerConfig partitionerConfig = mock(PartitionerConfig.class);
    when(partitionerConfig.getAmplificationFactor()).thenReturn(1);
    when(version.getPartitionerConfig()).thenReturn(partitionerConfig);
    when(store.getVersion(anyInt())).thenReturn(Optional.of(version));
    when(metadataRepo.getStoreOrThrow(anyString())).thenReturn(store);
    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    RocksDBServerConfig dbServerConfig = mock(RocksDBServerConfig.class);
    doReturn(dbServerConfig).when(serverConfig).getRocksDBServerConfig();

    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2));
    try {
      AtomicInteger errorLogCount = new AtomicInteger();
      // Adding a custom appender to track the count of error logs we are interested in
      ErrorCountAppender errorCountAppender = new ErrorCountAppender.Builder().setErrorMessageCounter(errorLogCount)
          .setExceptionMessage(exceptionMessage)
          .build();
      errorCountAppender.start();

      LoggerContext ctx = ((LoggerContext) LogManager.getContext(false));
      Configuration config = ctx.getConfiguration();
      config.addLoggerAppender(
          (org.apache.logging.log4j.core.Logger) LogManager.getLogger(StorageReadRequestsHandler.class),
          errorCountAppender);

      // Actual test
      StorageReadRequestsHandler testHandler = new StorageReadRequestsHandler(
          threadPoolExecutor,
          threadPoolExecutor,
          testRepository,
          metadataRepo,
          schemaRepo,
          mockMetadataRetriever,
          null,
          false,
          false,
          10,
          serverConfig,
          mock(StorageEngineBackedCompressorFactory.class),
          Optional.empty());
      testHandler.channelRead(mockCtx, testRequest);

      waitUntilStorageExecutionHandlerRespond(outputArray);

      // parsing of response
      Assert.assertEquals(outputArray.size(), 1);
      HttpShortcutResponse obj = (HttpShortcutResponse) outputArray.get(0);

      // Verification
      Assert.assertEquals(obj.getStatus(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      Assert.assertEquals(obj.getMessage(), exceptionMessage);

      // Asserting that the exception got logged
      Assert.assertTrue(errorLogCount.get() > 0);
    } finally {
      TestUtils.shutdownExecutor(threadPoolExecutor);
    }
  }

  @Test
  public static void testAdminRequestsPassInStorageExecutionHandler() throws Exception {
    String topic = "test_store_v1";
    int expectedPartitionId = 12345;
    List<Object> outputArray = new ArrayList<Object>();

    // [0]""/[1]"action"/[2]"store_version"/[3]"dump_ingestion_state"
    String uri = "/" + QueryAction.ADMIN.toString().toLowerCase() + "/" + topic + "/"
        + ServerAdminAction.DUMP_INGESTION_STATE.toString();
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    AdminRequest testRequest = AdminRequest.parseAdminHttpRequest(httpRequest);

    // Mock the AdminResponse from ingestion task
    AdminResponse expectedAdminResponse = new AdminResponse();
    PartitionConsumptionState state = new PartitionConsumptionState(
        expectedPartitionId,
        1,
        new OffsetRecord(AvroProtocolDefinition.PARTITION_STATE.getSerializer()),
        false);
    expectedAdminResponse.addPartitionConsumptionState(state);

    MetadataRetriever mockMetadataRetriever = mock(MetadataRetriever.class);
    doReturn(expectedAdminResponse).when(mockMetadataRetriever).getConsumptionSnapshots(eq(topic), any());

    /**
     * Capture the output written by {@link StorageReadRequestsHandler}
     */
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2));
    try {
      VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
      RocksDBServerConfig dbServerConfig = mock(RocksDBServerConfig.class);
      doReturn(dbServerConfig).when(serverConfig).getRocksDBServerConfig();

      // Actual test
      StorageReadRequestsHandler testHandler = new StorageReadRequestsHandler(
          threadPoolExecutor,
          threadPoolExecutor,
          mock(StorageEngineRepository.class),
          mock(ReadOnlyStoreRepository.class),
          mock(ReadOnlySchemaRepository.class),
          mockMetadataRetriever,
          null,
          false,
          false,
          10,
          serverConfig,
          mock(StorageEngineBackedCompressorFactory.class),
          Optional.empty());
      testHandler.channelRead(mockCtx, testRequest);

      waitUntilStorageExecutionHandlerRespond(outputArray);

      // parsing of response
      Assert.assertEquals(outputArray.size(), 1);
      Assert.assertTrue(outputArray.get(0) instanceof AdminResponse);
      AdminResponse obj = (AdminResponse) outputArray.get(0);

      // Verification
      Assert.assertEquals(obj.getResponseRecord().partitionConsumptionStates.size(), 1);
      Assert.assertEquals(obj.getResponseRecord().partitionConsumptionStates.get(0).partitionId, expectedPartitionId);
      Assert.assertEquals(
          obj.getResponseSchemaIdHeader(),
          AvroProtocolDefinition.SERVER_ADMIN_RESPONSE_V1.getCurrentProtocolVersion());
    } finally {
      TestUtils.shutdownExecutor(threadPoolExecutor);
    }
  }

  @Test
  public static void testMetadataFetchRequestsPassInStorageExecutionHandler() throws Exception {
    String storeName = "test_store_name";
    Map<CharSequence, CharSequence> keySchema = Collections.singletonMap("test_key_schema_id", "test_key_schema");
    Map<CharSequence, CharSequence> valueSchemas =
        Collections.singletonMap("test_value_schema_id", "test_value_schemas");
    List<Object> outputArray = new ArrayList<Object>();

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

    MetadataRetriever mockMetadataRetriever = mock(MetadataRetriever.class);
    doReturn(expectedMetadataResponse).when(mockMetadataRetriever).getMetadata(eq(storeName));

    /**
     * Capture the output written by {@link StorageReadRequestsHandler}
     */
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2));
    try {
      VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
      RocksDBServerConfig dbServerConfig = mock(RocksDBServerConfig.class);
      doReturn(dbServerConfig).when(serverConfig).getRocksDBServerConfig();

      // Actual test
      StorageReadRequestsHandler testHandler = new StorageReadRequestsHandler(
          threadPoolExecutor,
          threadPoolExecutor,
          mock(StorageEngineRepository.class),
          mock(ReadOnlyStoreRepository.class),
          mock(ReadOnlySchemaRepository.class),
          mockMetadataRetriever,
          null,
          false,
          false,
          10,
          serverConfig,
          mock(StorageEngineBackedCompressorFactory.class),
          Optional.empty());
      testHandler.channelRead(mockCtx, testRequest);

      waitUntilStorageExecutionHandlerRespond(outputArray);

      Assert.assertEquals(outputArray.size(), 1);
      Assert.assertTrue(outputArray.get(0) instanceof MetadataResponse);
      MetadataResponse obj = (MetadataResponse) outputArray.get(0);

      // Verification
      Assert.assertEquals(obj.getResponseRecord().getVersionMetadata(), versionProperties);
      Assert.assertEquals(obj.getResponseRecord().getKeySchema(), keySchema);
      Assert.assertEquals(obj.getResponseRecord().getValueSchemas(), valueSchemas);
    } finally {
      TestUtils.shutdownExecutor(threadPoolExecutor);
    }
  }

  @Test
  public static void testUnrecognizedRequestInStorageExecutionHandler() throws Exception {
    List<Object> outputArray = new ArrayList<Object>();
    Object unrecognizedRequest = new Object();

    /**
     * Capture the output written by {@link StorageReadRequestsHandler}
     */
    ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
    doReturn(new UnpooledByteBufAllocator(true)).when(mockCtx).alloc();
    when(mockCtx.writeAndFlush(any())).then(i -> {
      outputArray.add(i.getArguments()[0]);
      return null;
    });

    ThreadPoolExecutor threadPoolExecutor =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(2));
    try {
      VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
      RocksDBServerConfig dbServerConfig = mock(RocksDBServerConfig.class);
      doReturn(dbServerConfig).when(serverConfig).getRocksDBServerConfig();

      // Actual test
      StorageReadRequestsHandler testHandler = new StorageReadRequestsHandler(
          threadPoolExecutor,
          threadPoolExecutor,
          mock(StorageEngineRepository.class),
          mock(ReadOnlyStoreRepository.class),
          mock(ReadOnlySchemaRepository.class),
          mock(MetadataRetriever.class),
          null,
          false,
          false,
          10,
          serverConfig,
          mock(StorageEngineBackedCompressorFactory.class),
          Optional.empty());
      testHandler.channelRead(mockCtx, unrecognizedRequest);

      waitUntilStorageExecutionHandlerRespond(outputArray);

      Assert.assertEquals(outputArray.size(), 1);
      Assert.assertTrue(outputArray.get(0) instanceof HttpShortcutResponse);
      HttpShortcutResponse obj = (HttpShortcutResponse) outputArray.get(0);

      // Verification
      Assert.assertEquals(obj.getStatus(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      Assert.assertEquals(obj.getMessage(), "Unrecognized object in StorageExecutionHandler");
    } finally {
      TestUtils.shutdownExecutor(threadPoolExecutor);
    }
  }
}
