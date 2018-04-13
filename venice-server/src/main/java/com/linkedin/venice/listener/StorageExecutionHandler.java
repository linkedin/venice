package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.ReadResponse;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.MetadataRetriever;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.record.ValueRecord;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.queues.LabeledRunnable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.validation.constraints.NotNull;


/***
 * {@link StorageExecutionHandler} will take the incoming {@link RouterRequest}, and delegate the lookup request to
 * a thread pool {@link #executor}, which is being shared by all the requests.
 * Especially, this handler will execute parallel lookups for {@link MultiGetRouterRequestWrapper}.
 */
@ChannelHandler.Sharable
public class StorageExecutionHandler extends ChannelInboundHandlerAdapter {
  private final ExecutorService executor;
  private final StoreRepository storeRepository;
  private final MetadataRetriever metadataRetriever;
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
  private final ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(false);

  public StorageExecutionHandler(@NotNull ExecutorService executor, @NotNull StoreRepository storeRepository,
      @NotNull MetadataRetriever metadataRetriever) {
    this.executor = executor;
    this.storeRepository = storeRepository;
    this.metadataRetriever = metadataRetriever;
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
      executor.submit(new LabeledRunnable(request.getStoreName(), ()-> {
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
            default:
              throw new VeniceException("Unknown request type: " + request.getRequestType());
          }
          response.setStorageExecutionSubmissionWaitTime(submissionWaitTime);
          context.writeAndFlush(response);
        } catch (Exception e) {
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
        }
      }));
    } else {
      context.writeAndFlush(new HttpShortcutResponse("Unrecognized object in StorageExecutionHandler",
          HttpResponseStatus.INTERNAL_SERVER_ERROR));
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
    ValueRecord valueRecord = getValueRecord(store, partition, key, isChunked, response);
    double bdbQueryLatency = LatencyUtils.getLatencyInMS(queryStartTimeInNS);
    response.setValueRecord(valueRecord);
    response.setOffset(offset);
    response.setBdbQueryLatency(bdbQueryLatency);
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

    double bdbQueryLatency = LatencyUtils.getLatencyInMS(queryStartTimeInNS);
    responseWrapper.setBdbQueryLatency(bdbQueryLatency);

    // Offset data
    Set<Integer> partitionIdSet = new HashSet<>();
    keys.forEach(routerRequestKey -> {
      int partitionId = routerRequestKey.partitionId;
      if (!partitionIdSet.contains(partitionId)) {
        partitionIdSet.add(partitionId);
        Optional<Long> offsetObj = metadataRetriever.getOffset(topic, partitionId);
        long offset = offsetObj.isPresent() ? offsetObj.get() : OffsetRecord.LOWEST_OFFSET;
        responseWrapper.addPartitionOffsetMapping(partitionId, offset);
      }
    });

    return responseWrapper;
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

    byte[] bytesArrayKey;
    if (isChunked) {
      bytesArrayKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key);
    } else {
      bytesArrayKey = key.array();
    }
    MultiGetResponseRecordV1 record = getFromStorage(
        store,
        partition,
        bytesArrayKey,
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
    if (isChunked) {
      key = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key);
    }
    return getFromStorage(
        store,
        partition,
        key,
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
      byte[] key,
      ReadResponse response,
      FullBytesToValue<VALUE> fullBytesToValueFunction,
      ConstructAssembledValueContainer<ASSEMBLED_VALUE_CONTAINER> constructAssembledValueContainer,
      AddChunkIntoContainer addChunkIntoContainerFunction,
      GetSizeOfContainer getSizeFromContainerFunction,
      ContainerToValue<ASSEMBLED_VALUE_CONTAINER, VALUE> containerToValueFunction) {

    byte[] value = store.get(partition, key);
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
