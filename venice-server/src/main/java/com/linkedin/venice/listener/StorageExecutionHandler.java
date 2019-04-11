package com.linkedin.venice.listener;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compute.CosineSimilarityOperator;
import com.linkedin.venice.compute.DotProductOperator;
import com.linkedin.venice.compute.ReadComputeOperator;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequestV1;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.ComputeResponseWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.MultiKeyResponseWrapper;
import com.linkedin.venice.listener.response.ReadResponse;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.schema.avro.ComputablePrimitiveFloatList;
import com.linkedin.venice.schema.avro.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.storage.DiskHealthCheckService;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.utils.ComputeUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.queues.LabeledRunnable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.validation.constraints.NotNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.log4j.Logger;


/***
 * {@link StorageExecutionHandler} will take the incoming {@link RouterRequest}, and delegate the lookup request to
 * a thread pool {@link #executor}, which is being shared by all the requests.
 * Especially, this handler will execute parallel lookups for {@link MultiGetRouterRequestWrapper}.
 */
@ChannelHandler.Sharable
public class StorageExecutionHandler extends ChannelInboundHandlerAdapter {
  private static final Logger logger = Logger.getLogger(StorageExecutionHandler.class);

  private final DiskHealthCheckService diskHealthCheckService;
  private final ExecutorService executor;
  private final ExecutorService computeExecutor;
  private final StoreRepository storeRepository;
  private final ReadOnlySchemaRepository schemaRepo;
  private final MetadataRetriever metadataRetriever;
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
  private final ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(false);

  private final Map<Utf8, Schema> computeResultSchemaCache;

  private final Map<Integer, ReadComputeOperator> computeOperators = new HashMap<Integer, ReadComputeOperator>(){
    {
      put(ComputeOperationType.DOT_PRODUCT.getValue(), new DotProductOperator());
      put(ComputeOperationType.COSINE_SIMILARITY.getValue(), new CosineSimilarityOperator());
    }
  };

  public StorageExecutionHandler(@NotNull ExecutorService executor, @NotNull ExecutorService computeExecutor,
      @NotNull StoreRepository storeRepository, @NotNull ReadOnlySchemaRepository schemaRepo,
      @NotNull MetadataRetriever metadataRetriever, @NotNull DiskHealthCheckService healthCheckService) {
    this.executor = executor;
    this.computeExecutor = computeExecutor;
    this.storeRepository = storeRepository;
    this.schemaRepo = schemaRepo;
    this.metadataRetriever = metadataRetriever;
    this.diskHealthCheckService = healthCheckService;

    this.computeResultSchemaCache = new VeniceConcurrentHashMap<>();
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
    final long preSubmissionTimeNs = System.nanoTime();

    /**
     * N.B.: This is the only place in the entire class where we submit things into the {@link executor}.
     *
     * The reason for this is two-fold:
     *
     * 1. We want to make the {@link StorageExecutionHandler} fully non-blocking as far as Netty (which
     *    is the one calling this function) is concerned. Therefore, it is beneficial to fork off the
     *    work into the executor from the very beginning.
     * 2. By making the execution asynchronous from the beginning, we can simplify the rest of the class
     *    by making every other function a blocking one. If there is a desire to introduce additional
     *    concurrency in the rest of the class (i.e.: to make batch gets or large value re-assembly
     *    parallel), then it would be good to carefully consider whether this is a premature optimization,
     *    and if not, whether these additional operations should be performed in the same executor or in
     *    a secondary one, so as to not starve the primary requests. Furthermore, it should be considered
     *    whether it might be more beneficial to do streaming of these large response use cases, rather
     *    than parallel operations gated behind a synchronization barrier before any of the response can
     *    be sent out.
     */


    if (message instanceof RouterRequest) {
      RouterRequest request = (RouterRequest) message;
      getExecutor(request.getRequestType()).submit(new LabeledRunnable(request.getStoreName(), ()-> {
        double submissionWaitTime = LatencyUtils.getLatencyInMS(preSubmissionTimeNs);
        ReadResponse response;
        try {
          switch (request.getRequestType()) {
            case SINGLE_GET:
              response = handleSingleGetRequest((GetRouterRequest) request);
              break;
            case MULTI_GET:
              response = handleMultiGetRequest((MultiGetRouterRequestWrapper) request);
              break;
            case COMPUTE:
              response = handleComputeRequest((ComputeRouterRequestWrapper) message);
              break;
            default:
              throw new VeniceException("Unknown request type: " + request.getRequestType());
          }
          response.setStorageExecutionSubmissionWaitTime(submissionWaitTime);
          context.writeAndFlush(response);
        } catch (Exception e) {
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
        }
      }));
    } else if (message instanceof HealthCheckRequest) {
      if (diskHealthCheckService.isDiskHealthy()) {
        context.writeAndFlush(new HttpShortcutResponse("OK", HttpResponseStatus.OK));
      } else {
        context.writeAndFlush(new HttpShortcutResponse("Venice storage node hardware is not healthy!", HttpResponseStatus.INTERNAL_SERVER_ERROR));
        logger.error("Disk is not healthy according to the disk health check service: " + diskHealthCheckService.getErrorMessage());
      }

    } else {
      context.writeAndFlush(new HttpShortcutResponse("Unrecognized object in StorageExecutionHandler",
          HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }

  }

  private ExecutorService getExecutor(RequestType requestType) {
    switch (requestType) {
      case SINGLE_GET:
      case MULTI_GET:
        return executor;
      case COMPUTE:
        return computeExecutor;
      default:
        throw new VeniceException("Request type " + requestType + " is not supported.");
    }
  }

  private static IOFileFilter listEverything = new IOFileFilter() {
    @Override
    public boolean accept(File file) {
      return true;
    }

    @Override
    public boolean accept(File dir, String name) {
      return true;
    }
  };

  private ReadResponse handleSingleGetRequest(GetRouterRequest request) {
    int partition = request.getPartition();
    String topic = request.getResourceName();
    boolean isChunked = metadataRetriever.isStoreVersionChunked(topic);
    byte[] key = request.getKeyBytes();

    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);

    Optional<Long> offsetObj = metadataRetriever.getOffset(topic, partition);
    long offset = offsetObj.isPresent() ? offsetObj.get() : OffsetRecord.LOWEST_OFFSET;
    StorageResponseObject response = new StorageResponseObject();
    response.setCompressionStrategy(metadataRetriever.getStoreVersionCompressionStrategy(topic));

    long queryStartTimeInNS = System.nanoTime();
    ValueRecord valueRecord = getValueRecord(store, partition, key, isChunked, response);
    response.setValueRecord(valueRecord);
    response.setOffset(offset);
    response.setDatabaseLookupLatency(LatencyUtils.getLatencyInMS(queryStartTimeInNS));
    return response;
  }

  private ReadResponse handleMultiGetRequest(MultiGetRouterRequestWrapper request) {
    String topic = request.getResourceName();
    Iterable<MultiGetRouterRequestKeyV1> keys = request.getKeys();
    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);

    long queryStartTimeInNS = System.nanoTime();

    MultiGetResponseWrapper responseWrapper = new MultiGetResponseWrapper();
    responseWrapper.setCompressionStrategy(metadataRetriever.getStoreVersionCompressionStrategy(topic));
    boolean isChunked = metadataRetriever.isStoreVersionChunked(topic);
    for (MultiGetRouterRequestKeyV1 key : keys) {
      MultiGetResponseRecordV1 record =
          getMultiGetResponseRecordV1(store, key.partitionId, key.keyBytes, key.keyIndex, isChunked, responseWrapper);
      if (null != record) {
        responseWrapper.addRecord(record);
      }
    }

    responseWrapper.setDatabaseLookupLatency(LatencyUtils.getLatencyInMS(queryStartTimeInNS));

    // Offset data
    Set<Integer> partitionIdSet = new HashSet<>();
    keys.forEach(routerRequestKey ->
        addPartitionOffsetMapping(topic, routerRequestKey.partitionId, partitionIdSet, responseWrapper));

    return responseWrapper;
  }

  private ReadResponse handleComputeRequest(ComputeRouterRequestWrapper request) {
    String topic = request.getResourceName();
    String storeName = request.getStoreName();
    Iterable<ComputeRouterRequestKeyV1> keys = request.getKeys();
    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);

    // deserialize all value to the latest schema
    Schema latestValueSchema = this.schemaRepo.getLatestValueSchema(storeName).getSchema();
    ComputeRequestV1 computeRequest = request.getComputeRequest();

    // try to get the result schema from the cache
    Utf8 computeResultSchemaStr = (Utf8) computeRequest.resultSchemaStr;
    Schema computeResultSchema = computeResultSchemaCache.get(computeResultSchemaStr);
    if (computeResultSchema == null) {
      computeResultSchema = Schema.parse(computeResultSchemaStr.toString());
      // sanity check on the result schema
      ComputeUtils.checkResultSchema(computeResultSchema, latestValueSchema, (List) computeRequest.operations);
      computeResultSchemaCache.putIfAbsent(computeResultSchemaStr, computeResultSchema);
    }

    ComputeResponseWrapper responseWrapper = new ComputeResponseWrapper();
    CompressionStrategy compressionStrategy = metadataRetriever.getStoreVersionCompressionStrategy(topic);
    boolean isChunked = metadataRetriever.isStoreVersionChunked(topic);

    // The following metrics will get incremented for each record processed in computeResult()
    responseWrapper.setReadComputeDeserializationLatency(0.0);
    responseWrapper.setDatabaseLookupLatency(0.0);
    responseWrapper.setReadComputeSerializationLatency(0.0);
    responseWrapper.setReadComputeLatency(0.0);

    responseWrapper.setCompressionStrategy(CompressionStrategy.NO_OP);

    // Reuse the same value record and result record instances for all values
    GenericRecord valueRecord = new GenericData.Record(latestValueSchema);
    GenericRecord resultRecord = new GenericData.Record(computeResultSchema);
    RecordSerializer<GenericRecord> resultSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(computeResultSchema);
    for (ComputeRouterRequestKeyV1 key : keys) {
      clearFieldsInReusedRecord(resultRecord, computeResultSchema);
      ComputeResponseRecordV1 record = computeResult(store,
                                                     storeName,
                                                     key.keyBytes,
                                                     key.keyIndex,
                                                     key.partitionId,
                                                     computeRequest.operations,
                                                     compressionStrategy,
                                                     latestValueSchema,
                                                     computeResultSchema,
                                                     resultSerializer,
                                                     valueRecord,
                                                     resultRecord,
                                                     isChunked,
                                                     responseWrapper);
      if (null != record) {
        responseWrapper.addRecord(record);
      }
    }

    // Offset data
    Set<Integer> partitionIdSet = new HashSet<>();
    keys.forEach(routerRequestKey ->
        addPartitionOffsetMapping(topic, routerRequestKey.partitionId, partitionIdSet, responseWrapper));

    return responseWrapper;
  }

  private void addPartitionOffsetMapping(String topic, int partitionId, Set<Integer> partitionIdSet,
      MultiKeyResponseWrapper responseWrapper) {
    if (!partitionIdSet.contains(partitionId)) {
      partitionIdSet.add(partitionId);
      Optional<Long> offsetObj = metadataRetriever.getOffset(topic, partitionId);
      long offset = offsetObj.isPresent() ? offsetObj.get() : OffsetRecord.LOWEST_OFFSET;
      responseWrapper.addPartitionOffsetMapping(partitionId, offset);
    }
  }

  private void clearFieldsInReusedRecord(GenericRecord record, Schema schema) {
    for (int idx = 0; idx < schema.getFields().size(); idx++) {
      record.put(idx, null);
    }
  }

  private ComputeResponseRecordV1 computeResult(
      AbstractStorageEngine store,
      String storeName,
      ByteBuffer key,
      final int keyIndex,
      int partition,
      List<Object> operations,
      CompressionStrategy compressionStrategy,
      Schema latestValueSchema,
      Schema computeResultSchema,
      RecordSerializer<GenericRecord> resultSerializer,
      GenericRecord valueRecord,
      GenericRecord resultRecord,
      boolean isChunked,
      ComputeResponseWrapper response) {
    // get the raw bytes of the value from the key first
    ByteBuffer keyBuffer;
    if (isChunked) {
      keyBuffer = ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key));
    } else {
      keyBuffer = key;
    }

    /**
     * Reuse MultiGetResponseRecordV1 while getting data from storage engine; MultiGetResponseRecordV1 contains
     * useful information like schemaId of this record.
     */
    long databaseLookupStartTimeInNS = System.nanoTime();
    MultiGetResponseRecordV1 record = getFromStorage(
        store,
        partition,
        keyBuffer,
        response,
        fullBytesToMultiGetResponseRecordV1,
        constructNioByteBuffer,
        addChunkIntoNioByteBuffer,
        getSizeOfNioByteBuffer,
        containerToMultiGetResponseRecord);
    response.addDatabaseLookupLatency(LatencyUtils.getLatencyInMS(databaseLookupStartTimeInNS));
    if (null == record) {
      return null;
    }

    // deserialize raw byte value to GenericRecord
    long deserializeStartTimeInNS = System.nanoTime();
    RecordDeserializer<GenericRecord> deserializer =
        ComputableSerializerDeserializerFactory.getComputableAvroGenericDeserializer(
            this.schemaRepo.getValueSchema(storeName, record.schemaId).getSchema(), // writer schema
            latestValueSchema                                                       // reader schema
        );

    ByteBuffer decompressedData;
    if (compressionStrategy != CompressionStrategy.NO_OP) {
      // decompress the data first
      try {
        decompressedData = CompressorFactory.getCompressor(compressionStrategy)
            .decompress(
                record.value.array(),    // data
                record.value.position(), // offset
                record.value.remaining() // length
            );

      } catch (IOException e) {
        throw new VeniceException("failed to decompress data. Store: " + storeName, e);
      }
    } else {
      decompressedData = record.value;
    }
    BinaryDecoder decoder = DecoderFactory.defaultFactory()
                            .binaryDecoder(
                                decompressedData.array(),       // data
                                decompressedData.position(),    // offset
                                decompressedData.remaining(),   // length
                                null                      // reused binary decoder
                            );

    // reuse the same Generic Record instance for all keys
    valueRecord = deserializer.deserialize(valueRecord, decoder);
    response.addReadComputeDeserializationLatency(LatencyUtils.getLatencyInMS(deserializeStartTimeInNS));

    long computeStartTimeInNS = System.nanoTime();
    Map<String, String> computationErrorMap = new HashMap<>();

    // go through all operation
    for (Object operation : operations) {
      ComputeOperation op = (ComputeOperation) operation;
      computeOperators.get(op.operationType).compute(op, valueRecord, resultRecord, computationErrorMap);
    }

    // fill the empty field in result schema
    for (Schema.Field field : computeResultSchema.getFields()) {
      if (resultRecord.get(field.pos()) == null) {
        if (field.name().equals(VeniceConstants.VENICE_COMPUTATION_ERROR_MAP_FIELD_NAME)) {
          resultRecord.put(field.pos(), computationErrorMap);
        } else {
          // project from value record
          resultRecord.put(field.pos(), valueRecord.get(field.name()));
        }
      }
    }
    response.addReadComputeLatency(LatencyUtils.getLatencyInMS(computeStartTimeInNS));

    // create a response record
    ComputeResponseRecordV1 responseRecord = new ComputeResponseRecordV1();
    responseRecord.keyIndex = keyIndex;

    // serialize the compute result
    long serializeStartTimeInNS = System.nanoTime();
    responseRecord.value = ByteBuffer.wrap(resultSerializer.serialize(resultRecord));
    response.addReadComputeSerializationLatency(LatencyUtils.getLatencyInMS(serializeStartTimeInNS));

    return responseRecord;
  }

  /**
   * Handles the retrieval of a {@link MultiGetResponseRecordV1}, handling everything specific to this
   * code path, including chunking the key and dealing with {@link ByteBuffer}.
   *
   * This is used by the batch get code path.
   */
  private MultiGetResponseRecordV1 getMultiGetResponseRecordV1(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer key,
      final int keyIndex,
      boolean isChunked,
      MultiGetResponseWrapper response) {

    if (isChunked) {
      key = ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key));
    }
    MultiGetResponseRecordV1 record = getFromStorage(
        store,
        partition,
        key,
        response,
        fullBytesToMultiGetResponseRecordV1,
        constructNioByteBuffer,
        addChunkIntoNioByteBuffer,
        getSizeOfNioByteBuffer,
        containerToMultiGetResponseRecord);
    if (record != null) {
      record.keyIndex = keyIndex;
    }
    return record;
  }

  private static final FullBytesToValue fullBytesToMultiGetResponseRecordV1 = (schemaId, fullBytes) -> {
    MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
    /** N.B.: Does not need any repositioning, as opposed to {@link containerToMultiGetResponseRecord} */
    record.value = ValueRecord.parseDataAsNIOByteBuffer(fullBytes);
    record.schemaId = schemaId;
    return record;
  };
  private static final ConstructAssembledValueContainer constructNioByteBuffer = chunkedValueManifest -> ByteBuffer.allocate(chunkedValueManifest.size);
  private static final AddChunkIntoContainer<ByteBuffer> addChunkIntoNioByteBuffer = (byteBuffer, chunkIndex, valueChunk) ->
      byteBuffer.put(valueChunk, ValueRecord.SCHEMA_HEADER_LENGTH, valueChunk.length - ValueRecord.SCHEMA_HEADER_LENGTH);
  private static final GetSizeOfContainer<ByteBuffer> getSizeOfNioByteBuffer = byteBuffer -> byteBuffer.position();
  private static final ContainerToValue<ByteBuffer, MultiGetResponseRecordV1> containerToMultiGetResponseRecord = (schemaId, byteBuffer) -> {
    MultiGetResponseRecordV1 record = new MultiGetResponseRecordV1();
    /**
     * For re-assembled large values, it is necessary to reposition the {@link ByteBuffer} back to the
     * beginning of its content, otherwise the Avro encoder will skip this content.
     *
     * Note that this only occurs for a ByteBuffer we've been writing into gradually (i.e.: during chunk
     * re-assembly) but does not occur for a ByteBuffer that has been created by wrapping a single byte
     * array (i.e.: as is the case for small values, in {@link fullBytesToMultiGetResponseRecordV1}).
     * Doing this re-positioning for small values would cause another type of problem, because for these
     * (wrapping) ByteBuffer instances, the position needs to remain set to the starting offset (within
     * the backing array) which was originally specified at construction time...
     */
    byteBuffer.position(0);
    record.value = byteBuffer;
    record.schemaId = schemaId;
    return record;
  };

  /**
   * Handles the retrieval of a {@link ValueRecord}, handling everything specific to this
   * code path, including chunking the key and dealing with {@link ByteBuf} or {@link CompositeByteBuf}.
   *
   * This is used by the single get code path.
   */
  private ValueRecord getValueRecord(AbstractStorageEngine store, int partition, byte[] key, boolean isChunked, StorageResponseObject response) {
    ByteBuffer keyBuffer = null;
    if (isChunked) {
      keyBuffer = ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key));
    } else {
      keyBuffer = ByteBuffer.wrap(key);
    }
    return getFromStorage(
        store,
        partition,
        keyBuffer,
        response,
        fullBytesToValueRecord,
        constructCompositeByteBuf,
        addChunkIntoCompositeByteBuf,
        getSizeOfCompositeByteBuf,
        containerToValueRecord);
  }
  private static final FullBytesToValue fullBytesToValueRecord = (schemaId, fullBytes) -> ValueRecord.parseAndCreate(fullBytes);
  private static final ConstructAssembledValueContainer constructCompositeByteBuf = chunkedValueManifest ->
      Unpooled.compositeBuffer(chunkedValueManifest.keysWithChunkIdSuffix.size());
  private static final AddChunkIntoContainer<CompositeByteBuf> addChunkIntoCompositeByteBuf = (o, chunkIndex, valueChunk) ->
      o.addComponent(true, chunkIndex, ValueRecord.parseDataAsByteBuf(valueChunk));
  private static final GetSizeOfContainer<CompositeByteBuf> getSizeOfCompositeByteBuf = byteBuf -> byteBuf.readableBytes();
  private static final ContainerToValue<CompositeByteBuf, ValueRecord> containerToValueRecord = (schemaId, byteBuf) -> ValueRecord.create(schemaId, byteBuf);

  // The code below is general-purpose storage engine handling and chunking re-assembly. The specifics
  // of single gets vs batch gets are abstracted away by a handful of functional interfaces.

  /**
   * Used to wrap a small {@param fullBytes} value fetched from the storage engine into the right type
   * of {@param VALUE} class needed by the query code.
   */
  private interface FullBytesToValue<VALUE> {
    VALUE construct(int schemaId, byte[] fullBytes);
  }

  /**
   * Used to construct the right kind of {@param ASSEMBLED_VALUE_CONTAINER} container (according to
   * the query code) to hold a large value which needs to be incrementally re-assembled from many
   * smaller chunks.
   */
  private interface ConstructAssembledValueContainer<ASSEMBLED_VALUE_CONTAINER> {
    ASSEMBLED_VALUE_CONTAINER construct(ChunkedValueManifest chunkedValueManifest);
  }

  /**
   * Used to incrementally add a {@param valueChunk} into the {@param ASSEMBLED_VALUE_CONTAINER}
   * container.
   */
  private interface AddChunkIntoContainer<ASSEMBLED_VALUE_CONTAINER> {
    void add(ASSEMBLED_VALUE_CONTAINER container, int chunkIndex, byte[] valueChunk);
  }

  /**
   * Used to get the size in bytes of a fully assembled {@param ASSEMBLED_VALUE_CONTAINER}, in
   * order to do a sanity check before returning it to the query code.
   */
  private interface GetSizeOfContainer<ASSEMBLED_VALUE_CONTAINER> {
    int sizeOf(ASSEMBLED_VALUE_CONTAINER container);
  }

  /**
   * Used to wrap a large value re-assembled with the use of a {@param ASSEMBLED_VALUE_CONTAINER}
   * into the right type of {@param VALUE} class needed by the query code.
   */
  private interface ContainerToValue<ASSEMBLED_VALUE_CONTAINER, VALUE> {
    VALUE construct(int schemaId, ASSEMBLED_VALUE_CONTAINER container);
  }

  /**
   * Fetches the value associated with the given key, and potentially re-assembles it, if it is
   * a chunked value. This function should not be called directly, from the query code, as it expects
   * the key to be properly formatted already. Use of one these simpler functions instead:
   *
   * @see #getValueRecord(AbstractStorageEngine, int, byte[], boolean, StorageResponseObject)
   * @see #getMultiGetResponseRecordV1(AbstractStorageEngine, int, ByteBuffer, int, boolean, MultiGetResponseWrapper)
   *
   * This code makes use of several functional interfaces in order to abstract away the different needs
   * of the single get and batch get query paths.
   */
  private <VALUE, ASSEMBLED_VALUE_CONTAINER> VALUE getFromStorage(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer keyBuffer,
      ReadResponse response,
      FullBytesToValue<VALUE> fullBytesToValueFunction,
      ConstructAssembledValueContainer<ASSEMBLED_VALUE_CONTAINER> constructAssembledValueContainer,
      AddChunkIntoContainer addChunkIntoContainerFunction,
      GetSizeOfContainer getSizeFromContainerFunction,
      ContainerToValue<ASSEMBLED_VALUE_CONTAINER, VALUE> containerToValueFunction) {

    byte[] value = store.get(partition, keyBuffer);
    if (null == value) {
      return null;
    }
    int schemaId = ValueRecord.parseSchemaId(value);

    if (schemaId > 0) {
      // User-defined schema, thus not a chunked value. Early termination.
      return fullBytesToValueFunction.construct(schemaId, value);
    } else if (schemaId != AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      throw new VeniceException("Found a record with invalid schema ID: " + schemaId);
    }

    // End of initial sanity checks. We have chunked value, so we need to fetch all chunks

    ChunkedValueManifest chunkedValueManifest = chunkedValueManifestSerializer.deserialize(value, schemaId);
    ASSEMBLED_VALUE_CONTAINER assembledValueContainer = constructAssembledValueContainer.construct(chunkedValueManifest);

    for (int chunkIndex = 0; chunkIndex < chunkedValueManifest.keysWithChunkIdSuffix.size(); chunkIndex++) {
      // N.B.: This is done sequentially. Originally, each chunk was fetched concurrently in the same executor
      // as the main queries, but this might cause deadlocks, so we are now doing it sequentially. If we want to
      // optimize large value retrieval in the future, it's unclear whether the concurrent retrieval approach
      // is optimal (as opposed to streaming the response out incrementally, for example). Since this is a
      // premature optimization, we are not addressing it right now.
      byte[] valueChunk = store.get(partition, chunkedValueManifest.keysWithChunkIdSuffix.get(chunkIndex).array());

      if (null == valueChunk) {
        throw new VeniceException(
            "Chunk not found in " + getExceptionMessageDetails(store, partition, chunkIndex));
      } else if (ValueRecord.parseSchemaId(valueChunk) != AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
        throw new VeniceException(
            "Did not get the chunk schema ID while attempting to retrieve a chunk! " + "Instead, got schema ID: " + ValueRecord.parseSchemaId(valueChunk) + " from "
                + getExceptionMessageDetails(store, partition, chunkIndex));
      }

      addChunkIntoContainerFunction.add(assembledValueContainer, chunkIndex, valueChunk);
    }

    // Sanity check based on size...
    int actualSize = getSizeFromContainerFunction.sizeOf(assembledValueContainer);
    if (actualSize != chunkedValueManifest.size) {
      throw new VeniceException(
          "The fully assembled large value does not have the expected size! " + "actualSize: " + actualSize + ", chunkedValueManifest.size: " + chunkedValueManifest.size
              + ", " + getExceptionMessageDetails(store, partition, null));
    }

    response.incrementMultiChunkLargeValueCount();

    return containerToValueFunction.construct(chunkedValueManifest.schemaId, assembledValueContainer);
  }

  private String getExceptionMessageDetails(AbstractStorageEngine store, int partition, Integer chunkIndex) {
    String message = "store: " + store.getName() + ", partition: " + partition;
    if (chunkIndex != null) {
      message += ", chunk index: " + chunkIndex;
    }
    message += ".";
    return message;
  }
}
