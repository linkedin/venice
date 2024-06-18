package com.linkedin.venice.listener;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.listener.response.AdminResponse;
import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.listener.response.TopicPartitionIngestionContextResponse;
import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.davinci.storage.IngestionMetadataRetriever;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.chunking.BatchGetChunkingAdapter;
import com.linkedin.davinci.storage.chunking.GenericRecordChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.cleaner.ResourceReadUsageTracker;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.compute.protocol.request.ComputeOperation;
import com.linkedin.venice.compute.protocol.request.ComputeRequest;
import com.linkedin.venice.compute.protocol.request.enums.ComputeOperationType;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.OperationNotAllowedException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.listener.request.AdminRequest;
import com.linkedin.venice.listener.request.ComputeRouterRequestWrapper;
import com.linkedin.venice.listener.request.CurrentVersionRequest;
import com.linkedin.venice.listener.request.DictionaryFetchRequest;
import com.linkedin.venice.listener.request.GetRouterRequest;
import com.linkedin.venice.listener.request.HealthCheckRequest;
import com.linkedin.venice.listener.request.MetadataFetchRequest;
import com.linkedin.venice.listener.request.MultiGetRouterRequestWrapper;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.listener.request.TopicPartitionIngestionContextRequest;
import com.linkedin.venice.listener.response.BinaryResponse;
import com.linkedin.venice.listener.response.ComputeResponseWrapper;
import com.linkedin.venice.listener.response.HttpShortcutResponse;
import com.linkedin.venice.listener.response.MultiGetResponseWrapper;
import com.linkedin.venice.listener.response.StorageResponseObject;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.streaming.StreamingConstants;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.AvroRecordUtils;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ComplementSet;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpResponseStatus;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/***
 * {@link StorageReadRequestHandler} will take the incoming read requests from router{@link RouterRequest}, and delegate
 * the lookup request to a thread pool {@link #executor}, which is being shared by all the requests. Especially, this
 * handler will execute parallel lookups for {@link MultiGetRouterRequestWrapper}.
 */
@ChannelHandler.Sharable
public class StorageReadRequestHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(StorageReadRequestHandler.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private final DiskHealthCheckService diskHealthCheckService;
  private final ThreadPoolExecutor executor;
  private final ThreadPoolExecutor computeExecutor;
  private final StorageEngineRepository storageEngineRepository;
  private final ReadOnlyStoreRepository metadataRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final IngestionMetadataRetriever ingestionMetadataRetriever;
  private final ReadMetadataRetriever readMetadataRetriever;
  private final Map<Utf8, Schema> computeResultSchemaCache;
  private final boolean fastAvroEnabled;
  private final Function<Schema, RecordSerializer<GenericRecord>> genericSerializerGetter;
  private final boolean parallelBatchGetEnabled;
  private final int parallelBatchGetChunkSize;
  private final boolean keyValueProfilingEnabled;
  private final VeniceServerConfig serverConfig;
  private final Map<String, PerStoreVersionState> perStoreVersionStateMap = new VeniceConcurrentHashMap<>();
  private final Map<String, StoreDeserializerCache<GenericRecord>> storeDeserializerCacheMap =
      new VeniceConcurrentHashMap<>();
  private final StorageEngineBackedCompressorFactory compressorFactory;
  private final Optional<ResourceReadUsageTracker> resourceReadUsageTracker;

  private static class PerStoreVersionState {
    final StoreDeserializerCache<GenericRecord> storeDeserializerCache;
    AbstractStorageEngine storageEngine;

    public PerStoreVersionState(
        AbstractStorageEngine storageEngine,
        StoreDeserializerCache<GenericRecord> storeDeserializerCache) {
      this.storageEngine = storageEngine;
      this.storeDeserializerCache = storeDeserializerCache;
    }
  }

  private static class ReusableObjects {
    /**
     * When constructing a {@link BinaryDecoder}, we pass in this 16 bytes array because if we pass anything
     * less than that, it would end up getting discarded by the ByteArrayByteSource's constructor, a new byte
     * array created, and the content of the one passed in would be copied into the newly constructed one.
     * Therefore, it seems more efficient, in terms of GC, to statically allocate a 16 bytes array and keep
     * re-using it to construct decoders. Since we always end up re-configuring the decoder and not actually
     * using its initial value, it shouldn't cause any issue to share it.
     */
    private static final byte[] BINARY_DECODER_PARAM = new byte[16];

    // reuse buffer for rocksDB value object
    final ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);

    // LRU cache for storing schema->record map for object reuse of value and result record
    final LinkedHashMap<Schema, GenericRecord> valueRecordMap =
        new LinkedHashMap<Schema, GenericRecord>(100, 0.75f, true) {
          protected boolean removeEldestEntry(Map.Entry<Schema, GenericRecord> eldest) {
            return size() > 100;
          }
        };

    final LinkedHashMap<Schema, GenericRecord> resultRecordMap =
        new LinkedHashMap<Schema, GenericRecord>(100, 0.75f, true) {
          protected boolean removeEldestEntry(Map.Entry<Schema, GenericRecord> eldest) {
            return size() > 100;
          }
        };

    final BinaryDecoder binaryDecoder =
        AvroCompatibilityHelper.newBinaryDecoder(BINARY_DECODER_PARAM, 0, BINARY_DECODER_PARAM.length, null);

    final Map<String, Object> computeContext = new HashMap<>();
  }

  private final ThreadLocal<ReusableObjects> threadLocalReusableObjects = ThreadLocal.withInitial(ReusableObjects::new);

  public StorageReadRequestHandler(
      ThreadPoolExecutor executor,
      ThreadPoolExecutor computeExecutor,
      StorageEngineRepository storageEngineRepository,
      ReadOnlyStoreRepository metadataStoreRepository,
      ReadOnlySchemaRepository schemaRepository,
      IngestionMetadataRetriever ingestionMetadataRetriever,
      ReadMetadataRetriever readMetadataRetriever,
      DiskHealthCheckService healthCheckService,
      boolean fastAvroEnabled,
      boolean parallelBatchGetEnabled,
      int parallelBatchGetChunkSize,
      VeniceServerConfig serverConfig,
      StorageEngineBackedCompressorFactory compressorFactory,
      Optional<ResourceReadUsageTracker> resourceReadUsageTracker) {
    this.executor = executor;
    this.computeExecutor = computeExecutor;
    this.storageEngineRepository = storageEngineRepository;
    this.metadataRepository = metadataStoreRepository;
    this.schemaRepository = schemaRepository;
    this.ingestionMetadataRetriever = ingestionMetadataRetriever;
    this.readMetadataRetriever = readMetadataRetriever;
    this.diskHealthCheckService = healthCheckService;
    this.fastAvroEnabled = fastAvroEnabled;
    this.genericSerializerGetter = fastAvroEnabled
        ? FastSerializerDeserializerFactory::getFastAvroGenericSerializer
        : SerializerDeserializerFactory::getAvroGenericSerializer;
    this.computeResultSchemaCache = new VeniceConcurrentHashMap<>();
    this.parallelBatchGetEnabled = parallelBatchGetEnabled;
    this.parallelBatchGetChunkSize = parallelBatchGetChunkSize;
    this.keyValueProfilingEnabled = serverConfig.isKeyValueProfilingEnabled();
    this.serverConfig = serverConfig;
    this.compressorFactory = compressorFactory;
    this.resourceReadUsageTracker = resourceReadUsageTracker;
  }

  @Override
  public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
    final long preSubmissionTimeNs = System.nanoTime();

    /**
     * N.B.: This is the only place in the entire class where we submit things into the {@link executor}.
     *
     * The reason for this is two-fold:
     *
     * 1. We want to make the {@link StorageReadRequestHandler} fully non-blocking as far as Netty (which
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
      resourceReadUsageTracker.ifPresent(tracker -> tracker.recordReadUsage(request.getResourceName()));
      // Check before putting the request to the intermediate queue
      if (request.shouldRequestBeTerminatedEarly()) {
        // Try to make the response short
        VeniceRequestEarlyTerminationException earlyTerminationException =
            new VeniceRequestEarlyTerminationException(request.getStoreName());
        context.writeAndFlush(
            new HttpShortcutResponse(
                earlyTerminationException.getMessage(),
                earlyTerminationException.getHttpResponseStatus()));
        return;
      }
      /**
       * For now, we are evaluating whether parallel lookup is good overall or not.
       * Eventually, we either pick up the new parallel implementation or keep the original one, so it is fine
       * to have some duplicate code for the time-being.
       */
      if (parallelBatchGetEnabled && request.getRequestType().equals(RequestType.MULTI_GET)) {
        handleMultiGetRequestInParallel((MultiGetRouterRequestWrapper) request, parallelBatchGetChunkSize)
            .whenComplete((v, e) -> {
              if (e != null) {
                if (e instanceof VeniceRequestEarlyTerminationException) {
                  VeniceRequestEarlyTerminationException earlyTerminationException =
                      (VeniceRequestEarlyTerminationException) e;
                  context.writeAndFlush(
                      new HttpShortcutResponse(
                          earlyTerminationException.getMessage(),
                          earlyTerminationException.getHttpResponseStatus()));
                } else if (e instanceof VeniceNoStoreException) {
                  HttpResponseStatus status = getHttpResponseStatus((VeniceNoStoreException) e);
                  context.writeAndFlush(
                      new HttpShortcutResponse(
                          "No storage exists for: " + ((VeniceNoStoreException) e).getStoreName(),
                          status));
                } else {
                  LOGGER.error("Exception thrown in parallel batch get for {}", request.getResourceName(), e);
                  HttpShortcutResponse shortcutResponse =
                      new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
                  shortcutResponse.setMisroutedStoreVersion(checkMisroutedStoreVersionRequest(request));
                  context.writeAndFlush(shortcutResponse);
                }
              } else {
                context.writeAndFlush(v);
              }
            });
        return;
      }

      final ThreadPoolExecutor executor = getExecutor(request.getRequestType());
      executor.submit(() -> {
        try {
          if (request.shouldRequestBeTerminatedEarly()) {
            throw new VeniceRequestEarlyTerminationException(request.getStoreName());
          }
          double submissionWaitTime = LatencyUtils.getElapsedTimeFromNSToMS(preSubmissionTimeNs);
          int queueLen = executor.getQueue().size();
          ReadResponse response;
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
          response.setStorageExecutionQueueLen(queueLen);
          response.setRCU(ReadQuotaEnforcementHandler.getRcu(request));
          if (request.isStreamingRequest()) {
            response.setStreamingResponse();
          }
          context.writeAndFlush(response);
        } catch (VeniceNoStoreException e) {
          String msg = "No storage exists for store: " + e.getStoreName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.error(msg, e);
          }
          HttpResponseStatus status = getHttpResponseStatus(e);
          context.writeAndFlush(new HttpShortcutResponse("No storage exists for: " + e.getStoreName(), status));
        } catch (VeniceRequestEarlyTerminationException e) {
          String msg = "Request timed out for store: " + e.getStoreName();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.error(msg, e);
          }
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.REQUEST_TIMEOUT));
        } catch (OperationNotAllowedException e) {
          String msg = "METHOD_NOT_ALLOWED: " + e.getMessage();
          if (!REDUNDANT_LOGGING_FILTER.isRedundantException(msg)) {
            LOGGER.error(msg, e);
          }
          context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.METHOD_NOT_ALLOWED));
        } catch (Exception e) {
          LOGGER.error("Exception thrown for {}", request.getResourceName(), e);
          HttpShortcutResponse shortcutResponse =
              new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
          shortcutResponse.setMisroutedStoreVersion(checkMisroutedStoreVersionRequest(request));
          context.writeAndFlush(shortcutResponse);
        }
      });

    } else if (message instanceof HealthCheckRequest) {
      if (diskHealthCheckService.isDiskHealthy()) {
        context.writeAndFlush(new HttpShortcutResponse("OK", HttpResponseStatus.OK));
      } else {
        context.writeAndFlush(
            new HttpShortcutResponse(
                "Venice storage node hardware is not healthy!",
                HttpResponseStatus.INTERNAL_SERVER_ERROR));
        LOGGER.error(
            "Disk is not healthy according to the disk health check service: {}",
            diskHealthCheckService.getErrorMessage());
      }
    } else if (message instanceof DictionaryFetchRequest) {
      BinaryResponse response = handleDictionaryFetchRequest((DictionaryFetchRequest) message);
      context.writeAndFlush(response);
    } else if (message instanceof AdminRequest) {
      AdminResponse response = handleServerAdminRequest((AdminRequest) message);
      context.writeAndFlush(response);
    } else if (message instanceof MetadataFetchRequest) {
      try {
        MetadataResponse response = handleMetadataFetchRequest((MetadataFetchRequest) message);
        context.writeAndFlush(response);
      } catch (UnsupportedOperationException e) {
        LOGGER.warn(
            "Metadata requested by a storage node read quota not enabled store: {}",
            ((MetadataFetchRequest) message).getStoreName());
        context.writeAndFlush(new HttpShortcutResponse(e.getMessage(), HttpResponseStatus.FORBIDDEN));
      }
    } else if (message instanceof CurrentVersionRequest) {
      ServerCurrentVersionResponse response = handleCurrentVersionRequest((CurrentVersionRequest) message);
      context.writeAndFlush(response);
    } else if (message instanceof TopicPartitionIngestionContextRequest) {
      TopicPartitionIngestionContextResponse response =
          handleTopicPartitionIngestionContextRequest((TopicPartitionIngestionContextRequest) message);
      context.writeAndFlush(response);
    } else {
      context.writeAndFlush(
          new HttpShortcutResponse(
              "Unrecognized object in StorageExecutionHandler",
              HttpResponseStatus.INTERNAL_SERVER_ERROR));
    }
  }

  private HttpResponseStatus getHttpResponseStatus(VeniceNoStoreException e) {
    String topic = e.getStoreName();
    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int version = Version.parseVersionFromKafkaTopicName(topic);
    Store store = metadataRepository.getStore(storeName);

    if (store == null || store.getCurrentVersion() != version) {
      return HttpResponseStatus.BAD_REQUEST;
    }

    // return SERVICE_UNAVAILABLE to kick off error retry in router when store version resource exists
    return HttpResponseStatus.SERVICE_UNAVAILABLE;
  }

  /**
   * Best effort check for the purpose of reporting misrouted store version metric when the request errors.
   */
  private boolean checkMisroutedStoreVersionRequest(RouterRequest request) {
    boolean misrouted = false;
    Store store = metadataRepository.getStore(request.getStoreName());
    if (store != null) {
      Version version = store.getVersion(Version.parseVersionFromVersionTopicName(request.getResourceName()));
      if (version == null) {
        misrouted = true;
      }
    }
    return misrouted;
  }

  private ThreadPoolExecutor getExecutor(RequestType requestType) {
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

  private PerStoreVersionState getPerStoreVersionState(String storeVersion) {
    PerStoreVersionState s = perStoreVersionStateMap.computeIfAbsent(storeVersion, this::generatePerStoreVersionState);
    if (s.storageEngine.isClosed()) {
      /**
       * This scenario can happen if the last partition hosted on this server got dropped by Helix, for example in a
       * case where a store has a small number of partition-replicas spread across a larger number of servers, and a
       * rebalance happens. In such case, we refresh the storage engine by getting a reference to the latest one from
       * the {@link storageEngineRepository}.
       */
      s.storageEngine = getStorageEngineOrThrow(storeVersion);
    }
    return s;
  }

  private PerStoreVersionState generatePerStoreVersionState(String storeVersion) {
    String storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
    AbstractStorageEngine storageEngine = getStorageEngineOrThrow(storeVersion);
    StoreDeserializerCache<GenericRecord> storeDeserializerCache = storeDeserializerCacheMap.computeIfAbsent(
        storeName,
        s -> new AvroStoreDeserializerCache<>(this.schemaRepository, s, this.fastAvroEnabled));
    return new PerStoreVersionState(storageEngine, storeDeserializerCache);
  }

  private AbstractStorageEngine getStorageEngineOrThrow(String storeVersion) {
    AbstractStorageEngine storageEngine = storageEngineRepository.getLocalStorageEngine(storeVersion);
    if (storageEngine == null) {
      throw new VeniceNoStoreException(storeVersion);
    }
    return storageEngine;
  }

  public ReadResponse handleSingleGetRequest(GetRouterRequest request) {
    String topic = request.getResourceName();
    PerStoreVersionState perStoreVersionState = getPerStoreVersionState(topic);
    byte[] key = request.getKeyBytes();

    AbstractStorageEngine storageEngine = perStoreVersionState.storageEngine;
    boolean isChunked = storageEngine.isChunked();
    StorageResponseObject response = new StorageResponseObject();
    response.setCompressionStrategy(storageEngine.getCompressionStrategy());
    response.setDatabaseLookupLatency(0);

    ValueRecord valueRecord =
        SingleGetChunkingAdapter.get(storageEngine, request.getPartition(), key, isChunked, response);
    response.setValueRecord(valueRecord);

    if (keyValueProfilingEnabled) {
      response.setKeySizeList(IntLists.singleton(key.length));
      response.setValueSizeList(IntLists.singleton(response.isFound() ? valueRecord.getDataSize() : -1));
    }

    return response;
  }

  private CompletableFuture<ReadResponse> handleMultiGetRequestInParallel(
      MultiGetRouterRequestWrapper request,
      int parallelChunkSize) {
    String topic = request.getResourceName();
    Iterable<MultiGetRouterRequestKeyV1> keys = request.getKeys();
    PerStoreVersionState perStoreVersionState = getPerStoreVersionState(topic);
    AbstractStorageEngine storageEngine = perStoreVersionState.storageEngine;

    MultiGetResponseWrapper responseWrapper = new MultiGetResponseWrapper(request.getKeyCount());
    responseWrapper.setCompressionStrategy(storageEngine.getCompressionStrategy());
    responseWrapper.setDatabaseLookupLatency(0);
    boolean isChunked = storageEngine.isChunked();

    ExecutorService executorService = getExecutor(RequestType.MULTI_GET);
    if (!(keys instanceof ArrayList)) {
      throw new VeniceException("'keys' in MultiGetResponseWrapper should be an ArrayList");
    }
    final ArrayList<MultiGetRouterRequestKeyV1> keyList = (ArrayList<MultiGetRouterRequestKeyV1>) keys;
    int totalKeyNum = keyList.size();
    int splitSize = (int) Math.ceil((double) totalKeyNum / parallelChunkSize);

    ReentrantLock requestLock = new ReentrantLock();
    CompletableFuture[] chunkFutures = new CompletableFuture[splitSize];

    IntList responseKeySizeList = keyValueProfilingEnabled ? new IntArrayList(totalKeyNum) : null;
    IntList responseValueSizeList = keyValueProfilingEnabled ? new IntArrayList(totalKeyNum) : null;

    for (int cur = 0; cur < splitSize; ++cur) {
      final int finalCur = cur;
      chunkFutures[cur] = CompletableFuture.runAsync(() -> {
        if (request.shouldRequestBeTerminatedEarly()) {
          throw new VeniceRequestEarlyTerminationException(request.getStoreName());
        }
        int startPos = finalCur * parallelChunkSize;
        int endPos = Math.min((finalCur + 1) * parallelChunkSize, totalKeyNum);
        for (int subChunkCur = startPos; subChunkCur < endPos; ++subChunkCur) {
          final MultiGetRouterRequestKeyV1 key = keyList.get(subChunkCur);
          if (responseKeySizeList != null) {
            responseKeySizeList.set(subChunkCur, key.keyBytes.remaining());
          }
          MultiGetResponseRecordV1 record =
              BatchGetChunkingAdapter.get(storageEngine, key.partitionId, key.keyBytes, isChunked, responseWrapper);
          if (record == null) {
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

          if (record != null) {
            if (responseValueSizeList != null) {
              responseValueSizeList.set(subChunkCur, record.value.remaining());
            }
            // TODO: streaming support in storage node
            requestLock.lock();
            try {
              responseWrapper.addRecord(record);
            } finally {
              requestLock.unlock();
            }
          } else {
            if (responseValueSizeList != null) {
              responseValueSizeList.set(subChunkCur, -1);
            }
          }
        }
      }, executorService);
    }

    return CompletableFuture.allOf(chunkFutures).handle((v, e) -> {
      if (e != null) {
        throw new VeniceException(e);
      }
      responseWrapper.setKeySizeList(responseKeySizeList);
      responseWrapper.setValueSizeList(responseValueSizeList);
      return responseWrapper;
    });
  }

  public ReadResponse handleMultiGetRequest(MultiGetRouterRequestWrapper request) {
    Iterable<MultiGetRouterRequestKeyV1> keys = request.getKeys();
    PerStoreVersionState perStoreVersionState = getPerStoreVersionState(request.getResourceName());
    AbstractStorageEngine storageEngine = perStoreVersionState.storageEngine;

    MultiGetResponseWrapper responseWrapper = new MultiGetResponseWrapper(request.getKeyCount());
    responseWrapper.setCompressionStrategy(storageEngine.getCompressionStrategy());
    responseWrapper.setDatabaseLookupLatency(0);
    boolean isChunked = storageEngine.isChunked();
    for (MultiGetRouterRequestKeyV1 key: keys) {
      MultiGetResponseRecordV1 record =
          BatchGetChunkingAdapter.get(storageEngine, key.partitionId, key.keyBytes, isChunked, responseWrapper);
      if (record == null) {
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

      if (record != null) {
        // TODO: streaming support in storage node
        responseWrapper.addRecord(record);
      }
    }
    return responseWrapper;
  }

  private ReadResponse handleComputeRequest(ComputeRouterRequestWrapper request) {
    if (!metadataRepository.isReadComputationEnabled(request.getStoreName())) {
      throw new OperationNotAllowedException(
          "Read compute is not enabled for the store. Please contact Venice team to enable the feature.");
    }
    SchemaEntry superSetOrLatestValueSchema = schemaRepository.getSupersetOrLatestValueSchema(request.getStoreName());
    SchemaEntry valueSchemaEntry = getComputeValueSchema(request, superSetOrLatestValueSchema);
    Schema resultSchema = getComputeResultSchema(request.getComputeRequest(), valueSchemaEntry.getSchema());
    RecordSerializer<GenericRecord> resultSerializer = genericSerializerGetter.apply(resultSchema);
    PerStoreVersionState storeVersion = getPerStoreVersionState(request.getResourceName());
    VeniceCompressor compressor =
        compressorFactory.getCompressor(storeVersion.storageEngine.getCompressionStrategy(), request.getResourceName());

    // Reuse the same value record and result record instances for all values
    ReusableObjects reusableObjects = threadLocalReusableObjects.get();
    GenericRecord reusableValueRecord =
        reusableObjects.valueRecordMap.computeIfAbsent(valueSchemaEntry.getSchema(), GenericData.Record::new);
    GenericRecord reusableResultRecord =
        reusableObjects.resultRecordMap.computeIfAbsent(resultSchema, GenericData.Record::new);
    reusableObjects.computeContext.clear();

    ComputeResponseWrapper response = new ComputeResponseWrapper(request.getKeyCount());
    List<ComputeOperation> operations = request.getComputeRequest().getOperations();
    List<Schema.Field> operationResultFields = ComputeUtils.getOperationResultFields(operations, resultSchema);
    int hits = 0;
    for (ComputeRouterRequestKeyV1 key: request.getKeys()) {
      AvroRecordUtils.clearRecord(reusableResultRecord);
      GenericRecord result = computeResult(
          operations,
          operationResultFields,
          storeVersion,
          key,
          reusableValueRecord,
          valueSchemaEntry.getId(),
          compressor,
          response,
          reusableObjects,
          reusableResultRecord);
      if (addComputationResult(response, key, result, resultSerializer, request.isStreamingRequest())) {
        hits++;
      }
    }
    incrementOperatorCounters(response, operations, hits);
    return response;
  }

  private BinaryResponse handleDictionaryFetchRequest(DictionaryFetchRequest request) {
    ByteBuffer dictionary = ingestionMetadataRetriever.getStoreVersionCompressionDictionary(request.getResourceName());
    return new BinaryResponse(dictionary);
  }

  private MetadataResponse handleMetadataFetchRequest(MetadataFetchRequest request) {
    return readMetadataRetriever.getMetadata(request.getStoreName());
  }

  private ServerCurrentVersionResponse handleCurrentVersionRequest(CurrentVersionRequest request) {
    return readMetadataRetriever.getCurrentVersionResponse(request.getStoreName());
  }

  private Schema getComputeResultSchema(ComputeRequest computeRequest, Schema valueSchema) {
    Utf8 resultSchemaStr = (Utf8) computeRequest.getResultSchemaStr();
    Schema resultSchema = computeResultSchemaCache.get(resultSchemaStr);
    if (resultSchema == null) {
      resultSchema = new Schema.Parser().parse(resultSchemaStr.toString());
      // Sanity check on the result schema
      ComputeUtils.checkResultSchema(resultSchema, valueSchema, computeRequest.getOperations());
      computeResultSchemaCache.putIfAbsent(resultSchemaStr, resultSchema);
    }
    return resultSchema;
  }

  private SchemaEntry getComputeValueSchema(
      ComputeRouterRequestWrapper request,
      SchemaEntry superSetOrLatestValueSchema) {
    return request.getValueSchemaId() != SchemaData.INVALID_VALUE_SCHEMA_ID
        ? schemaRepository.getValueSchema(request.getStoreName(), request.getValueSchemaId())
        : superSetOrLatestValueSchema;
  }

  /**
   * @return true if the result is not null, false otherwise
   */
  private boolean addComputationResult(
      ComputeResponseWrapper response,
      ComputeRouterRequestKeyV1 key,
      GenericRecord result,
      RecordSerializer<GenericRecord> resultSerializer,
      boolean isStreaming) {
    if (result != null) {
      long serializeStartTimeInNS = System.nanoTime();
      ComputeResponseRecordV1 record = new ComputeResponseRecordV1();
      record.keyIndex = key.getKeyIndex();
      record.value = ByteBuffer.wrap(resultSerializer.serialize(result));
      response.addReadComputeSerializationLatency(LatencyUtils.getElapsedTimeFromNSToMS(serializeStartTimeInNS));
      response.addReadComputeOutputSize(record.value.remaining());
      response.addRecord(record);
      return true;
    } else if (isStreaming) {
      // For streaming, we need to send back non-existing keys
      ComputeResponseRecordV1 record = new ComputeResponseRecordV1();
      // Negative key index to indicate non-existing key
      record.keyIndex = Math.negateExact(key.getKeyIndex());
      record.value = StreamingUtils.EMPTY_BYTE_BUFFER;
      response.addRecord(record);
    }
    return false;
  }

  private GenericRecord computeResult(
      List<ComputeOperation> operations,
      List<Schema.Field> operationResultFields,
      PerStoreVersionState storeVersion,
      ComputeRouterRequestKeyV1 key,
      GenericRecord reusableValueRecord,
      int readerSchemaId,
      VeniceCompressor compressor,
      ComputeResponseWrapper response,
      ReusableObjects reusableObjects,
      GenericRecord reusableResultRecord) {
    reusableValueRecord =
        readValueRecord(key, storeVersion, readerSchemaId, compressor, response, reusableObjects, reusableValueRecord);
    if (reusableValueRecord == null) {
      return null;
    }

    long computeStartTimeInNS = System.nanoTime();
    reusableResultRecord = ComputeUtils.computeResult(
        operations,
        operationResultFields,
        reusableObjects.computeContext,
        reusableValueRecord,
        reusableResultRecord);
    response.addReadComputeLatency(LatencyUtils.getElapsedTimeFromNSToMS(computeStartTimeInNS));
    return reusableResultRecord;
  }

  private GenericRecord readValueRecord(
      ComputeRouterRequestKeyV1 key,
      PerStoreVersionState storeVersion,
      int readerSchemaId,
      VeniceCompressor compressor,
      ReadResponse response,
      ReusableObjects reusableObjects,
      GenericRecord reusableValueRecord) {
    return GenericRecordChunkingAdapter.INSTANCE.get(
        storeVersion.storageEngine,
        key.getPartitionId(),
        ByteUtils.extractByteArray(key.getKeyBytes()),
        reusableObjects.byteBuffer,
        reusableValueRecord,
        reusableObjects.binaryDecoder,
        storeVersion.storageEngine.isChunked(),
        response,
        readerSchemaId,
        storeVersion.storeDeserializerCache,
        compressor);
  }

  private static void incrementOperatorCounters(
      ComputeResponseWrapper response,
      Iterable<ComputeOperation> operations,
      int hits) {
    for (ComputeOperation operation: operations) {
      switch (ComputeOperationType.valueOf(operation)) {
        case DOT_PRODUCT:
          response.incrementDotProductCount(hits);
          break;
        case COSINE_SIMILARITY:
          response.incrementCosineSimilarityCount(hits);
          break;
        case HADAMARD_PRODUCT:
          response.incrementHadamardProductCount(hits);
          break;
        case COUNT:
          response.incrementCountOperatorCount(hits);
          break;
      }
    }
  }

  private AdminResponse handleServerAdminRequest(AdminRequest adminRequest) {
    switch (adminRequest.getServerAdminAction()) {
      case DUMP_INGESTION_STATE:
        String topicName = adminRequest.getStoreVersion();
        Integer partitionId = adminRequest.getPartition();
        ComplementSet<Integer> partitions =
            (partitionId == null) ? ComplementSet.universalSet() : ComplementSet.of(partitionId);
        return ingestionMetadataRetriever.getConsumptionSnapshots(topicName, partitions);
      case DUMP_SERVER_CONFIGS:
        AdminResponse configResponse = new AdminResponse();
        if (this.serverConfig == null) {
          configResponse.setError(true);
          configResponse.setMessage("Server config doesn't exist");
        } else {
          configResponse.addServerConfigs(this.serverConfig.getClusterProperties().toProperties());
        }
        return configResponse;
      default:
        throw new VeniceException("Not a valid admin action: " + adminRequest.getServerAdminAction().toString());
    }
  }

  private TopicPartitionIngestionContextResponse handleTopicPartitionIngestionContextRequest(
      TopicPartitionIngestionContextRequest topicPartitionIngestionContextRequest) {
    Integer partition = topicPartitionIngestionContextRequest.getPartition();
    String versionTopic = topicPartitionIngestionContextRequest.getVersionTopic();
    String topicName = topicPartitionIngestionContextRequest.getTopic();
    return ingestionMetadataRetriever.getTopicPartitionIngestionContext(versionTopic, topicName, partition);
  }
}
