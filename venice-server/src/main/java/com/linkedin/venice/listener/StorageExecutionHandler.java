package com.linkedin.venice.listener;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.CosineSimilarityOperator;
import com.linkedin.venice.compute.DotProductOperator;
import com.linkedin.venice.compute.HadamardProductOperator;
import com.linkedin.venice.compute.ReadComputeOperator;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
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
import com.linkedin.venice.serializer.ComputableSerializerDeserializerFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.storage.DiskHealthCheckService;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.storage.chunking.ChunkingUtils;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.streaming.StreamingConstants;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.ComputeUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.queues.LabeledRunnable;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
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
  private final Map<Utf8, Schema> computeResultSchemaCache;
  private final boolean fastAvroEnabled;

  private final Map<Integer, ReadComputeOperator> computeOperators = new HashMap<Integer, ReadComputeOperator>() {
    {
      put(ComputeOperationType.DOT_PRODUCT.getValue(), new DotProductOperator());
      put(ComputeOperationType.COSINE_SIMILARITY.getValue(), new CosineSimilarityOperator());
      put(ComputeOperationType.HADAMARD_PRODUCT.getValue(), new HadamardProductOperator());
    }
  };

  public StorageExecutionHandler(@NotNull ExecutorService executor, @NotNull ExecutorService computeExecutor,
      @NotNull StoreRepository storeRepository, @NotNull ReadOnlySchemaRepository schemaRepository,
      @NotNull MetadataRetriever metadataRetriever, @NotNull DiskHealthCheckService healthCheckService,
      boolean fastAvroEnabled) {
    this.executor = executor;
    this.computeExecutor = computeExecutor;
    this.storeRepository = storeRepository;
    this.schemaRepo = schemaRepository;
    this.metadataRetriever = metadataRetriever;
    this.diskHealthCheckService = healthCheckService;
    this.fastAvroEnabled = fastAvroEnabled;
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
      getExecutor(request.getRequestType()).submit(new LabeledRunnable(request.getStoreName(), () -> {
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
          if (request.isStreamingRequest()) {
            response.setStreamingResponse();
          }
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
    ValueRecord valueRecord = ChunkingUtils.getValueRecord(store, partition, key, isChunked, response);
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
          ChunkingUtils.getMultiGetResponseRecordV1(store, key.partitionId, key.keyBytes, isChunked, responseWrapper);
      responseWrapper.setDatabaseLookupLatency(LatencyUtils.getLatencyInMS(queryStartTimeInNS));
      if (null == record) {
        if (request.isStreamingRequest()) {
          // For streaming, we would like to send back non-existing keys since the end-user won't know the status of
          // non-existing keys in the response if the response is partial.
          record = new MultiGetResponseRecordV1();
          // Negative key index to indicate the non-existing keys
          record.keyIndex = Math.negateExact(key.keyIndex);
          record.schemaId = StreamingConstants.NON_EXISTING_KEY_SCHEMA_ID;
          record.value = StreamingUtils.EMPTY_BYTE_BUFFER;
        }
      } else {
        record.keyIndex = key.keyIndex;
      }

      if (null != record) {
        // TODO: streaming support in storage node
        responseWrapper.addRecord(record);
      }
    }

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
    ComputeRequestWrapper computeRequestWrapper = request.getComputeRequest();

    // try to get the result schema from the cache
    Utf8 computeResultSchemaStr = (Utf8) computeRequestWrapper.getResultSchemaStr();
    Schema computeResultSchema = computeResultSchemaCache.get(computeResultSchemaStr);
    if (computeResultSchema == null) {
      computeResultSchema = Schema.parse(computeResultSchemaStr.toString());
      // sanity check on the result schema
      ComputeUtils.checkResultSchema(computeResultSchema, latestValueSchema, computeRequestWrapper.getComputeRequestVersion(), (List) computeRequestWrapper.getOperations());
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
    RecordSerializer<GenericRecord> resultSerializer;
    if (fastAvroEnabled) {
      resultSerializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(computeResultSchema);
    } else {
      resultSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(computeResultSchema);
    }
    Map<String, Object> globalContext = new HashMap<>();
    for (ComputeRouterRequestKeyV1 key : keys) {
      clearFieldsInReusedRecord(resultRecord, computeResultSchema);
      ComputeResponseRecordV1 record = computeResult(store,
                                                     storeName,
                                                     key.keyBytes,
                                                     key.keyIndex,
                                                     key.partitionId,
                                                     computeRequestWrapper.getComputeRequestVersion(),
                                                     computeRequestWrapper.getOperations(),
                                                     compressionStrategy,
                                                     latestValueSchema,
                                                     computeResultSchema,
                                                     resultSerializer,
                                                     valueRecord,
                                                     resultRecord,
                                                     isChunked,
                                                     request.isStreamingRequest(),
                                                     responseWrapper,
                                                     globalContext);
      if (null != record) {
        // TODO: streaming support in storage node
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
      int computeRequestVersion,
      List<Object> operations,
      CompressionStrategy compressionStrategy,
      Schema latestValueSchema,
      Schema computeResultSchema,
      RecordSerializer<GenericRecord> resultSerializer,
      GenericRecord valueRecord,
      GenericRecord resultRecord,
      boolean isChunked,
      boolean isStreaming,
      ComputeResponseWrapper response,
      Map<String, Object> globalContext) {
    /**
     * Reuse MultiGetResponseRecordV1 while getting data from storage engine; MultiGetResponseRecordV1 contains
     * useful information like schemaId of this record.
     */
    long databaseLookupStartTimeInNS = System.nanoTime();
    MultiGetResponseRecordV1 record =
        ChunkingUtils.getMultiGetResponseRecordV1(store, partition, key, isChunked, response);
    response.addDatabaseLookupLatency(LatencyUtils.getLatencyInMS(databaseLookupStartTimeInNS));
    if (null == record) {
      if (isStreaming) {
        // For streaming, we need to send back non-existing keys
        ComputeResponseRecordV1 computeResponseRecord = new ComputeResponseRecordV1();
        // Negative key index to indicate non-existing key
        computeResponseRecord.keyIndex = Math.negateExact(keyIndex);
        computeResponseRecord.value = StreamingUtils.EMPTY_BYTE_BUFFER;
        return computeResponseRecord;
      }
      return null;
    }

    // deserialize raw byte value to GenericRecord
    long deserializeStartTimeInNS = System.nanoTime();
    RecordDeserializer<GenericRecord> deserializer;
    Schema writerSchema = this.schemaRepo.getValueSchema(storeName, record.schemaId).getSchema();
    if (fastAvroEnabled) {
      deserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, latestValueSchema);
    } else {
      deserializer = ComputableSerializerDeserializerFactory.getComputableAvroGenericDeserializer(
          writerSchema,       // writer schema
          latestValueSchema   // reader schema
      );
    }

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
      computeOperators.get(op.operationType).compute(computeRequestVersion, op, valueRecord, resultRecord, computationErrorMap, globalContext);
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
}
