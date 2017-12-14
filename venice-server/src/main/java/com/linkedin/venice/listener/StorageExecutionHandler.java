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
import java.util.concurrent.CompletableFuture;
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
  private StoreRepository storeRepository;
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
    if (message instanceof RouterRequest) {
      RouterRequest request = (RouterRequest) message;
      try {
        switch (request.getRequestType()) {
          case SINGLE_GET:
            handleSingleGetRequest(context, (GetRouterRequest) request);
            break;
          case MULTI_GET:
            handleMultiGetRequest(context, (MultiGetRouterRequestWrapper) request);
            break;
          default:
            throw new VeniceException("Unknown request type: " + request.getRequestType());
        }
      } catch (Exception e) {
        context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
      }
    } else {
      context.writeAndFlush(new HttpShortcutResponse("Unrecognized object in StorageExecutionHandler",
          HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }
  }

  private void handleSingleGetRequest(ChannelHandlerContext context, GetRouterRequest request) {
    executor.submit(() -> {
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
      CompletableFuture<ValueRecord> valueRecordCompletableFuture = getValueRecord(store, partition, key, isChunked, response);
      valueRecordCompletableFuture.handle((valueRecord, throwable) -> {
        try {
          double bdbQueryLatency = LatencyUtils.getLatencyInMS(queryStartTimeInNS);
          response.setValueRecord(valueRecord);
          response.setOffset(offset);
          response.setBdbQueryLatency(bdbQueryLatency);
          context.writeAndFlush(response);
        } catch (Exception e) {
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
        }
        return valueRecord;
      });
    });
  }

  private void handleMultiGetRequest(ChannelHandlerContext context, MultiGetRouterRequestWrapper request) {
    String topic = request.getResourceName();
    Iterable<MultiGetRouterRequestKeyV1> keys = request.getKeys();
    AbstractStorageEngine store = storeRepository.getLocalStorageEngine(topic);

    long queryStartTimeInNS = System.nanoTime();
    MultiGetResponseWrapper responseWrapper = new MultiGetResponseWrapper();
    responseWrapper.setCompressionStrategy(metadataRetriever.getStoreVersionCompressionStrategy(topic));
    CompletableFuture<MultiGetResponseRecordV1>[] futureArray = new CompletableFuture[request.getKeyCount()];
    boolean isChunked = metadataRetriever.isStoreVersionChunked(topic);
    int i = 0;
    for (MultiGetRouterRequestKeyV1 key : keys) {
      int partitionId = key.partitionId;
      futureArray[i++] = getMultiGetResponseRecordV1(store, partitionId, key.keyBytes, key.keyIndex, isChunked, responseWrapper);
    }

    // Wait for all the lookup requests to complete (note: this is completed in the last future's thread)
    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futureArray);
    allDoneFuture.handle( (aVoid, throwable) -> {
      for (CompletableFuture<MultiGetResponseRecordV1> future : futureArray) {
        try {
          MultiGetResponseRecordV1 record = future.get();
          if (null != record) {
            responseWrapper.addRecord(record);
          }
        } catch (Exception e) {
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR));
          break;
        }
      }
      double bdbQueryLatency = LatencyUtils.getLatencyInMS(queryStartTimeInNS);
      responseWrapper.setBdbQueryLatency(bdbQueryLatency);

      // Offset data
      Set<Integer> partitionIdSet = new HashSet<>();
      keys.forEach( routerRequestKey -> {
        int partitionId = routerRequestKey.partitionId;
        if (!partitionIdSet.contains(partitionId)) {
          partitionIdSet.add(partitionId);
          Optional<Long> offsetObj = metadataRetriever.getOffset(topic, partitionId);
          long offset = offsetObj.isPresent() ? offsetObj.get() : OffsetRecord.LOWEST_OFFSET;
          responseWrapper.addPartitionOffsetMapping(partitionId, offset);
        }
      });

      context.writeAndFlush(responseWrapper);
      return aVoid;
    });
  }

  /**
   * Handles the retrieval of a {@link MultiGetResponseRecordV1}, handling everything specific to this
   * code path, including chunking the key and dealing with {@link ByteBuffer}.
   *
   * This is used by the batch get code path.
   */
  private CompletableFuture<MultiGetResponseRecordV1> getMultiGetResponseRecordV1(
      AbstractStorageEngine store,
      int partition,
      ByteBuffer key,
      int keyIndex,
      boolean isChunked,
      MultiGetResponseWrapper response) {
    byte[] bytesArrayKey;
    if (isChunked) {
      bytesArrayKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(key);
    } else {
      bytesArrayKey = key.array();
    }
    CompletableFuture<MultiGetResponseRecordV1> futureResponse = getFromStorage(
        store,
        partition,
        bytesArrayKey,
        response,
        fullBytesToMultiGetResponseRecordV1,
        constructNioByteBuffer,
        addChunkIntoNioByteBuffer,
        getSizeOfNioByteBuffer,
        containerToMultiGetResponseRecord);
    return futureResponse.thenApply(multiGetResponseRecordV1 -> {
      if (multiGetResponseRecordV1 != null) {
        multiGetResponseRecordV1.keyIndex = keyIndex;
      }
      return multiGetResponseRecordV1;
    });
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
  private CompletableFuture<ValueRecord> getValueRecord(AbstractStorageEngine store, int partition, byte[] key, boolean isChunked, StorageResponseObject response) {
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
  private <VALUE, ASSEMBLED_VALUE_CONTAINER> CompletableFuture<VALUE> getFromStorage(
      AbstractStorageEngine store,
      int partition,
      byte[] key,
      ReadResponse response,
      FullBytesToValue<VALUE> fullBytesToValueFunction,
      ConstructAssembledValueContainer<ASSEMBLED_VALUE_CONTAINER> constructAssembledValueContainer,
      AddChunkIntoContainer addChunkIntoContainerFunction,
      GetSizeOfContainer getSizeFromContainerFunction,
      ContainerToValue<ASSEMBLED_VALUE_CONTAINER, VALUE> containerToValueFunction) {

    byte[] value = store.get(partition, key); // This is the only blocking call of the function...
    if (null == value) {
      return CompletableFuture.completedFuture(null);
    }
    int schemaId = ValueRecord.parseSchemaId(value);

    if (schemaId > 0) {
      // User-defined schema, thus not a chunked value. Early termination.
      return CompletableFuture.completedFuture(fullBytesToValueFunction.construct(schemaId, value));
    } else if (schemaId != AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      throw new VeniceException("Found a record with invalid schema ID: " + schemaId);
    }

    // End of initial sanity checks. Now we need to fetch all chunks

    ChunkedValueManifest chunkedValueManifest = chunkedValueManifestSerializer.deserialize(value, schemaId);

    CompletableFuture<byte[]>[] futureArray = new CompletableFuture[chunkedValueManifest.keysWithChunkIdSuffix.size()];
    for (int chunkIndex = 0; chunkIndex < chunkedValueManifest.keysWithChunkIdSuffix.size(); chunkIndex++) {
      int finalChunkIndex = chunkIndex; // Must be final to be accessible from within the future.
      futureArray[finalChunkIndex] = CompletableFuture.supplyAsync(() -> {
        byte[] valueChunk = store.get(partition, chunkedValueManifest.keysWithChunkIdSuffix.get(finalChunkIndex).array());

        if (null == valueChunk) {
          throw new VeniceException("Chunk not found in "
              + getExceptionMessageDetails(store, partition, finalChunkIndex));
        } else if (ValueRecord.parseSchemaId(valueChunk) != AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
          throw new VeniceException("Did not get the chunk schema ID while attempting to retrieve a chunk! "
              + "Instead, got schema ID: " + ValueRecord.parseSchemaId(valueChunk) + " from "
              + getExceptionMessageDetails(store, partition, finalChunkIndex));
        }
        return valueChunk;
      }, executor);
    }

    // Re-assemble fetched chunks

    ASSEMBLED_VALUE_CONTAINER assembledValueContainer = constructAssembledValueContainer.construct(chunkedValueManifest);

    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futureArray);
    CompletableFuture<VALUE> valueRecordCompletableFuture = allDoneFuture.handle((aVoid, throwable) -> {
      for (int chunkIndex = 0; chunkIndex < chunkedValueManifest.keysWithChunkIdSuffix.size(); chunkIndex++) {
        CompletableFuture<byte[]> future = futureArray[chunkIndex];
        try {
          byte[] valueChunk = future.get();
          addChunkIntoContainerFunction.add(assembledValueContainer, chunkIndex, valueChunk);
        } catch (VeniceException e) {
          throw e;
        } catch (Exception e) {
          throw new VeniceException("Caught an exception while trying to fetch chunk from "
              + getExceptionMessageDetails(store, partition, chunkIndex), e);
        }
      }

      // Sanity check based on size...
      int actualSize = getSizeFromContainerFunction.sizeOf(assembledValueContainer);
      if (actualSize != chunkedValueManifest.size) {
        throw new VeniceException("The fully assembled large value does not have the expected size! "
            + "actualSize: " + actualSize
            + ", chunkedValueManifest.size: " + chunkedValueManifest.size
            + ", " + getExceptionMessageDetails(store, partition, null));
      }

      response.incrementMultiChunkLargeValueCount();

      return containerToValueFunction.construct(chunkedValueManifest.schemaId, assembledValueContainer);
    });

    return valueRecordCompletableFuture;
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
