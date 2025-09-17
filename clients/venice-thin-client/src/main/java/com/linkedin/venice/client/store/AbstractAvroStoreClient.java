package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.streaming.ClientComputeRecordStreamDecoder;
import com.linkedin.venice.client.store.streaming.DelegatingTrackingCallback;
import com.linkedin.venice.client.store.streaming.MultiGetRecordStreamDecoder;
import com.linkedin.venice.client.store.streaming.RecordStreamDecoder;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.TrackingStreamingCallback;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.ComputeUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.read.RequestHeadersProvider;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.VeniceSerializationException;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.digests.MD5Digest;


public abstract class AbstractAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractAvroStoreClient.class);
  public static final String TYPE_STORAGE = "storage";
  public static final String TYPE_COMPUTE = "compute";
  public static final String B64_FORMAT = "?f=b64";
  private final Map<Integer, RecordDeserializer<V>> deserializerCache = new VeniceConcurrentHashMap<>();

  private final ClientConfig clientConfig;
  protected final boolean needSchemaReader;
  /** Used to communicate with Venice backend to retrieve necessary store schemas */
  private SchemaReader schemaReader;
  // Key serializer
  protected volatile RecordSerializer<K> keySerializer;
  // Multi-get request serializer
  protected RecordSerializer<ByteBuffer> multiGetRequestSerializer;
  protected RecordSerializer<ByteBuffer> computeRequestClientKeySerializer;
  private RecordDeserializer<StreamingFooterRecordV1> streamingFooterRecordDeserializer;
  private TransportClient transportClient;
  private final Executor deserializationExecutor;
  private final CompressorFactory compressorFactory;
  private final String storageRequestPath;
  private final String computeRequestPath;
  private final AtomicBoolean remoteComputationAllowed = new AtomicBoolean(true);

  private volatile boolean isServiceDiscovered;

  private volatile boolean started = false;

  /**
   * Here is the details about the deadlock issue if deserialization logic is executed in the same R2 callback thread:
   * 1. A bunch of regular get requests are sent to Venice backend at the same time;
   * 2. All those requests return almost at the same time;
   * 3. All those requests will be blocked by the SchemaReader#fetchValueSchema(int)
   *    if the value schema is not in local cache;
   * 4. At this moment, each request will occupy one internal R2 callback thread, and the R2 callback threads will be
   *    exhausted if there are a lot of simultaneous regular get requests;
   * 5. Since all the R2 callback threads are blocked by the schema request, then there is no more R2 callback thread could
   *    handle the callback of the schema request;
   * 6. The deadlock issue happens;
   *
   * Loading all the value schemas during start won't solve this problem since the value schema could involve when store
   * client is running.
   * So we have to use a different set of threads in {@link SchemaReader} to avoid this issue.
   * Also, we don't want to use the default thread pool: CompletableFuture#useCommonPool since it is being shared,
   * and the deserialization could be blocked by the logic not belonging to Venice Client.
   **/
  static Executor DESERIALIZATION_EXECUTOR;

  public static synchronized Executor getDefaultDeserializationExecutor() {
    if (DESERIALIZATION_EXECUTOR == null) {
      // Half of process number of threads should be good enough, minimum 2
      int threadNum = Math.max(Runtime.getRuntime().availableProcessors() / 2, 2);

      DESERIALIZATION_EXECUTOR =
          Executors.newFixedThreadPool(threadNum, new DaemonThreadFactory("Venice-Store-Deserialization"));
    }

    return DESERIALIZATION_EXECUTOR;
  }

  protected AbstractAvroStoreClient(
      TransportClient transportClient,
      boolean needSchemaReader,
      ClientConfig clientConfig) {
    this.transportClient = transportClient;
    this.clientConfig = clientConfig;
    this.needSchemaReader = needSchemaReader;
    this.deserializationExecutor =
        Optional.ofNullable(clientConfig.getDeserializationExecutor()).orElse(getDefaultDeserializationExecutor());
    this.compressorFactory = new CompressorFactory();
    this.storageRequestPath = TYPE_STORAGE + "/" + clientConfig.getStoreName();
    this.computeRequestPath = TYPE_COMPUTE + "/" + clientConfig.getStoreName();
  }

  @Override
  public String getStoreName() {
    return clientConfig.getStoreName();
  }

  protected final ClientConfig getClientConfig() {
    return clientConfig;
  }

  protected TransportClient getTransportClient() {
    return transportClient;
  }

  @Override
  public SchemaReader getSchemaReader() {
    return schemaReader;
  }

  @Override
  public Executor getDeserializationExecutor() {
    return deserializationExecutor;
  }

  private String getStorageRequestPathForSingleKey(byte[] key) {
    String b64key = EncodingUtils.base64EncodeToString(key);
    return getStorageRequestPath() + "/" + b64key + B64_FORMAT;
  }

  private String getStorageRequestPath() {
    return storageRequestPath;
  }

  protected String getComputeRequestPath() {
    return computeRequestPath;
  }

  private void discoverD2Service(boolean retryOnFailure) {
    if (isServiceDiscovered) {
      return;
    }
    synchronized (this) {
      if (isServiceDiscovered) {
        return;
      }
      if (transportClient instanceof D2TransportClient) {
        D2TransportClient client = (D2TransportClient) transportClient;
        client.setServiceName(new D2ServiceDiscovery().find(client, getStoreName(), retryOnFailure).getD2Service());
      }
      isServiceDiscovered = true;
    }
  }

  /**
   * Clients using different protocols for deserialized data (e.g VSON, Proto, etc) can override this method to
   * serialize the respective POJO to Avro bytes
   * @return A serializer for key objects to Avro bytes
   */
  protected RecordSerializer<K> createKeySerializer() {
    return getClientConfig().isUseFastAvro()
        ? FastSerializerDeserializerFactory.getFastAvroGenericSerializer(getKeySchema())
        : SerializerDeserializerFactory.getAvroGenericSerializer(getKeySchema());
  }

  private void initSerializer() {
    // init key serializer
    if (needSchemaReader) {
      if (getSchemaReader() != null) {
        // init multi-get request serializer
        this.multiGetRequestSerializer = getClientConfig().isUseFastAvro()
            ? FastSerializerDeserializerFactory
                .getFastAvroGenericSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema())
            : SerializerDeserializerFactory
                .getAvroGenericSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
        // init compute request serializer
        this.computeRequestClientKeySerializer = getClientConfig().isUseFastAvro()
            ? FastSerializerDeserializerFactory
                .getFastAvroGenericSerializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema())
            : SerializerDeserializerFactory
                .getAvroGenericSerializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema());
        this.streamingFooterRecordDeserializer = getClientConfig().isUseFastAvro()
            ? FastSerializerDeserializerFactory
                .getFastAvroSpecificDeserializer(StreamingFooterRecordV1.SCHEMA$, StreamingFooterRecordV1.class)
            : SerializerDeserializerFactory
                .getAvroSpecificDeserializer(StreamingFooterRecordV1.SCHEMA$, StreamingFooterRecordV1.class);
        /**
         * It is intentional to initialize {@link keySerializer} at last, so that other serializers are ready to use
         * once {@link keySerializer} is ready.
         */
        this.keySerializer = createKeySerializer();
      } else {
        throw new VeniceClientException("SchemaReader is null while initializing serializer");
      }
    }
  }

  // For testing
  public String getRequestPathByKey(K key) throws VeniceClientException {
    tryStart(1);

    byte[] serializedKey = keySerializer.serialize(key);
    return getStorageRequestPathForSingleKey(serializedKey);
  }

  @Override
  public CompletableFuture<V> get(K key, Optional<ClientStats> stats, long preRequestTimeInNS)
      throws VeniceClientException {
    tryStart(1);

    byte[] serializedKey = keySerializer.serialize(key);
    String requestPath = getStorageRequestPathForSingleKey(serializedKey);
    CompletableFuture<V> valueFuture = new CompletableFuture<>();

    requestSubmissionWithStatsHandling(
        stats,
        preRequestTimeInNS,
        true,
        () -> transportClient.get(requestPath, RequestHeadersProvider.getThinClientGetHeaderMap()),
        (response, throwable, responseCompleteReporter) -> {
          try {
            if (throwable != null) {
              valueFuture.completeExceptionally(throwable);
            } else if (response == null) {
              // Doesn't exist
              valueFuture.complete(null);
            } else if (!response.isSchemaIdValid()) {
              valueFuture.completeExceptionally(
                  new VeniceClientException("No valid schema id received for single-get request!"));
            } else {
              CompressionStrategy compressionStrategy = response.getCompressionStrategy();
              long decompressionStartTime = System.nanoTime();
              ByteBuffer data = decompressRecord(compressionStrategy, ByteBuffer.wrap(response.getBody()));
              stats.ifPresent(
                  (clientStats) -> clientStats
                      .recordResponseDecompressionTime(LatencyUtils.getElapsedTimeFromNSToMS(decompressionStartTime)));
              RecordDeserializer<V> deserializer = getDataRecordDeserializerFromCache(response.getSchemaId());
              valueFuture.complete(tryToDeserialize(deserializer, data, response.getSchemaId(), key));
              responseCompleteReporter.report();
            }
          } catch (Exception e) {
            // Defensive code
            if (!valueFuture.isDone()) {
              valueFuture.completeExceptionally(e);
            }
          }
          return null;
        });

    return valueFuture;
  }

  @Override
  public CompletableFuture<byte[]> getRaw(
      String requestPath,
      Optional<ClientStats> stats,
      final long preRequestTimeInNS) {
    /**
     * Leveraging the following function to do safe D2 discovery for the following schema fetches.
     * And we could not use {@link #tryStart} since it will cause a dead loop:
     * {@link #getRaw} -> {@link #getKeySchema} -> {@link SchemaReader#getKeySchema} -> {@link #getRaw}
     */
    discoverD2Service(true);

    CompletableFuture<byte[]> valueFuture = new CompletableFuture<>();
    requestSubmissionWithStatsHandling(
        stats,
        preRequestTimeInNS,
        false,
        () -> transportClient.get(requestPath),
        (clientResponse, throwable, responseCompleteReporter) -> {
          try {
            if (throwable != null) {
              valueFuture.completeExceptionally(throwable);
            } else if (clientResponse == null) {
              // Doesn't exist
              valueFuture.complete(null);
            } else {
              valueFuture.complete(clientResponse.getBody());
              responseCompleteReporter.report();
            }
          } catch (Exception e) {
            // Defensive code
            if (!valueFuture.isDone()) {
              valueFuture.completeExceptionally(e);
            }
          }
          return null;
        });
    return valueFuture;
  }

  private <T> T tryToDeserialize(RecordDeserializer<T> dataDeserializer, ByteBuffer data, int writerSchemaId, K key) {
    return tryToDeserializeWithVerboseLogging(
        dataDeserializer,
        data,
        writerSchemaId,
        key,
        keySerializer,
        getSchemaReader(),
        LOGGER);
  }

  public static <T, K> T tryToDeserializeWithVerboseLogging(
      RecordDeserializer<T> dataDeserializer,
      ByteBuffer data,
      int writerSchemaId,
      K key,
      RecordSerializer<K> keySerializer,
      SchemaReader schemaReader,
      Logger LOGGER) {
    try {
      return dataDeserializer.deserialize(data);
    } catch (VeniceSerializationException e) {
      // N.B.: The code below is fairly defensive because we do not want to fail in the process of trying to
      // log debugging details. In practice, these try blocks should never catch anything.
      String latestSchemaId;
      try {
        // Hashing the value because 1) values tend to be too large for logs and 2) they might contain PII
        MD5Digest digest = new MD5Digest();
        byte[] valueChecksum = new byte[digest.getDigestSize()];
        digest.update(data.array(), data.position(), data.limit() - data.position());
        digest.doFinal(valueChecksum, 0);
      } catch (Exception e2) {
        LOGGER.error("Failed to compute value checksum", e2);
      }

      try {
        keySerializer.serialize(key);
      } catch (Exception e3) {
        LOGGER.error("Failed to serialize key", e3);
      }

      try {
        latestSchemaId = schemaReader.getLatestValueSchemaId().toString();
      } catch (Exception e4) {
        latestSchemaId = "Invalid";
        LOGGER.error("Failed to retrieve latest value schema ID.", e4);
      }

      LOGGER.error(
          "Caught a {}, will bubble up." + "RecordDeserializer: {}\nWriter schema ID: {}\nLatest schema ID: {}\n",
          VeniceSerializationException.class.getSimpleName(),
          dataDeserializer.getClass().getSimpleName(),
          (writerSchemaId == -1 ? "N/A" : writerSchemaId),
          latestSchemaId);
      throw e;
    }

  }

  /**
   * This function takes care of logging three metrics:
   *
   * - pre-request to pre-submission time (i.e.: request serialization)
   * - pre-submission to pre-handling time (i.e.: network round-trip)
   * - pre-handling to end time (i.e.: response deserialization)
   *
   * @param stats The {@link ClientStats} object to record into. Should be the one passed by the {@link StatTrackingStoreClient}.
   * @param preRequestTimeInNS The request start time. Should be the one passed by the {@link StatTrackingStoreClient}.
   * @param handleResponseOnDeserializationExecutor if true, will execute the {@param responseHandler} on the {@link deserializationExecutor}
   *                                                if false, will execute the {@param responseHandler} on the same thread.
   * @param requestSubmitter A closure which ONLY submits the request to the backend. Should not include any pre-submission work (i.e.: serialization).
   * @param responseHandler A closure which interprets the response from the backend (i.e.: deserialization).
   * @param <R> The return type of the {@param responseHandler}.
   * @return a {@link CompletableFuture<R>} wrapping the return of the {@param responseHandler}.
   * @throws VeniceClientException
   */
  private <R> CompletableFuture<R> requestSubmissionWithStatsHandling(
      Optional<ClientStats> stats,
      long preRequestTimeInNS,
      boolean handleResponseOnDeserializationExecutor,
      Supplier<CompletableFuture<TransportClientResponse>> requestSubmitter,
      ResponseHandler<R> responseHandler) throws VeniceClientException {
    final long preSubmitTimeInNS = System.nanoTime();
    CompletableFuture<TransportClientResponse> transportFuture = requestSubmitter.get();

    BiFunction<TransportClientResponse, Throwable, R> responseHandlerWithStats = (clientResponse, throwable) -> {
      // N.B.: All stats handling is async
      long preHandlingTimeNS = System.nanoTime();
      stats.ifPresent(
          clientStats -> clientStats
              .recordRequestSerializationTime(LatencyUtils.convertNSToMS(preSubmitTimeInNS - preRequestTimeInNS)));
      stats.ifPresent(
          clientStats -> clientStats.recordRequestSubmissionToResponseHandlingTime(
              LatencyUtils.convertNSToMS(preHandlingTimeNS - preSubmitTimeInNS)));

      return responseHandler.handle(
          clientResponse,
          throwable,
          () -> stats.ifPresent(
              clientStats -> clientStats
                  .recordResponseDeserializationTime(LatencyUtils.getElapsedTimeFromNSToMS(preHandlingTimeNS))));
    };

    if (handleResponseOnDeserializationExecutor) {
      return transportFuture.handleAsync(responseHandlerWithStats, getDeserializationExecutor());
    } else {
      return transportFuture.handle(responseHandlerWithStats);
    }
  }

  private interface ResponseHandler<R> {
    R handle(TransportClientResponse clientResponse, Throwable throwable, Reporter responseDeserializationComplete);
  }

  @Override
  public boolean isProjectionFieldValidationEnabled() {
    return getClientConfig().isProjectionFieldValidationEnabled();
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequest,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    if (handleCallbackForEmptyKeySet(keys, callback)) {
      // empty key set
      return;
    }
    tryStart(1);

    ClientComputeRecordStreamDecoder.Callback<K, GenericRecord> decoderCallback =
        new ClientComputeRecordStreamDecoder.Callback<K, GenericRecord>(
            DelegatingTrackingCallback.wrap((StreamingCallback) callback)) {
          private final Map<String, Object> sharedContext = new HashMap<>();

          @Override
          public void onRawRecordReceived(K key, GenericRecord value) {
            if (value != null) {
              value = ComputeUtils.computeResult(
                  computeRequest.getOperations(),
                  computeRequest.getOperationResultFields(),
                  sharedContext,
                  value,
                  resultSchema);
              getStats().ifPresent(stats -> stats.recordMultiGetFallback(1));
            }
            onRecordReceived(key, value);
          }

          @Override
          public void onRecordReceived(K key, GenericRecord value) {
            super.onRecordReceived(
                key,
                value != null ? new ComputeGenericRecord(value, computeRequest.getValueSchema()) : null);
          }

          @Override
          public void onRemoteComputeStateChange(boolean enabled) {
            remoteComputationAllowed.set(enabled);
          }
        };

    List<K> keyList = new ArrayList<>(keys);
    RecordStreamDecoder decoder = new ClientComputeRecordStreamDecoder<>(
        keyList,
        decoderCallback,
        getDeserializationExecutor(),
        streamingFooterRecordDeserializer,
        () -> getComputeResultRecordDeserializer(resultSchema),
        schemaId -> (RecordDeserializer) getDataRecordDeserializerFromCache(schemaId),
        this::decompressRecord);

    if (clientConfig.isRemoteComputationOnly() || remoteComputationAllowed.get()) {
      compute(computeRequest, keyList, decoder, decoderCallback.getStats());
    } else {
      /** Multi-get fallback is on until the router tells otherwise via {@link HttpConstants.VENICE_CLIENT_COMPUTE} */
      streamingBatchGet(keyList, decoder, decoderCallback.getStats());
    }
  }

  private void compute(
      ComputeRequestWrapper computeRequest,
      List<K> keyList,
      TransportClientStreamingCallback callback,
      Optional<ClientStats> stats) throws VeniceClientException {
    byte[] serializedRequest = serializeComputeRequest(computeRequest, keyList, stats);
    transportClient.streamPost(
        getComputeRequestPath(),
        RequestHeadersProvider.getStreamingComputeHeaderMap(
            keyList.size(),
            computeRequest.getValueSchemaID(),
            clientConfig.isRemoteComputationOnly()),
        serializedRequest,
        callback,
        keyList.size());
  }

  private byte[] serializeComputeRequest(
      ComputeRequestWrapper computeRequest,
      List<K> keyList,
      Optional<ClientStats> stats) {
    long preRequestSerializationNanos = System.nanoTime();
    List<ByteBuffer> serializedKeyList = new ArrayList<>(keyList.size());
    ByteBuffer serializedComputeRequest = ByteBuffer.wrap(computeRequest.serialize());
    for (K key: keyList) {
      serializedKeyList.add(ByteBuffer.wrap(keySerializer.serialize(key)));
    }
    byte[] result = computeRequestClientKeySerializer.serializeObjects(serializedKeyList, serializedComputeRequest);
    stats.ifPresent(
        s -> s.recordRequestSerializationTime(LatencyUtils.getElapsedTimeFromNSToMS(preRequestSerializationNanos)));
    return result;
  }

  @Override
  public void start() throws VeniceClientException {
    try {
      startWithExceptionThrownWhenFail();
    } catch (Exception e) {
      if (clientConfig.isForceClusterDiscoveryAtStartTime()) {
        if (e instanceof VeniceClientException) {
          throw (VeniceClientException) e;
        }
        throw new VeniceClientException(
            "Failed to initializing Venice Client: " + getStoreName() + " with error: " + e.getMessage(),
            e);
      }
      /**
       * We can't fail the start since the existing customers are relying on the best-effort start behavior.
       */
      LOGGER.warn(
          "Failed to start Venice Client for store: {} with error: {} and the actual start will be delayed",
          getStoreName(),
          e.getMessage());
    }
  }

  @Override
  public void startWithExceptionThrownWhenFail() {
    tryStart(10);
  }

  private void tryStart(int retryLimit) throws VeniceClientException {
    if (started) {
      return;
    }
    synchronized (this) {
      if (started) {
        return;
      }
      /**
       * The following function will try to warmup store metadata:
       * 1. Cluster discovery.
       * 2. Key/Value schemas.
       * 3. Necessary serializers.
       */
      discoverD2Service(retryLimit > 1);
      if (!needSchemaReader) {
        return;
      }
      /**
       * When the schema reader is disabled, we shouldn't try to initialize the serializers or refresh key/value schemas
       * since it might cause a deadlock.
       */
      this.schemaReader = new RouterBackedSchemaReader(
          this::getStoreClientForSchemaReader,
          getReaderSchema(),
          clientConfig.getPreferredSchemaFilter(),
          clientConfig.getSchemaRefreshPeriod(),
          null);

      Throwable lastException;
      int retryCount = 0;
      for (; retryCount < retryLimit; ++retryCount) {
        if (retryCount > 0) {
          try {
            // Short sleep interval should be good enough, and we assume the next retry could hit a different Router.
            Thread.sleep(50);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new VeniceException("Initialization of Venice client is interrupted");
          }
        }
        try {
          // Refresh the value schemas
          getSchemaReader().getLatestValueSchema();
          initSerializer();
          started = true;
          break;
        } catch (ServiceDiscoveryException e) {
          if (e.getCause() instanceof VeniceNoStoreException) {
            // No store error is not retriable
            throw e;
          }
          lastException = e.getCause();
        } catch (Exception e) {
          // Retry on other types of exceptions
          lastException = e;
        }
        if (retryCount == retryLimit - 1 && lastException != null) {
          // If we reach the retry limit, throw the last exception
          throw new VeniceClientException(
              "Failed to initialize Venice client for store: " + getStoreName() + " after " + retryLimit + " attempts",
              lastException);
        }
      }
    }
  }

  /**
   * The behavior of READ apis will be non-deterministic after `close` function is called.
   */
  @Override
  public void close() {
    IOUtils.closeQuietly(transportClient, LOGGER::error);
    IOUtils.closeQuietly(schemaReader, LOGGER::error);
    IOUtils.closeQuietly(compressorFactory, LOGGER::error);
  }

  protected Optional<Schema> getReaderSchema() {
    return Optional.empty();
  }

  /**
   * To avoid cycle dependency, we need to initialize another store client for schema reader.
   * @return
   * @throws VeniceClientException
   */
  protected abstract AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader();

  public abstract RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException;

  private RecordDeserializer<GenericRecord> getComputeResultRecordDeserializer(Schema resultSchema) {
    if (getClientConfig().isUseFastAvro()) {
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(resultSchema, resultSchema);
    }
    return SerializerDeserializerFactory.getAvroGenericDeserializer(resultSchema);
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(storeName: " + getStoreName() + ", transportClient: "
        + transportClient.toString() + ")";
  }

  @Override
  public Schema getKeySchema() {
    return getSchemaReader().getKeySchema();
  }

  @Deprecated
  @Override
  public Schema getLatestValueSchema() {
    return getSchemaReader().getLatestValueSchema();
  }

  private ByteBuffer decompressRecord(CompressionStrategy compressionStrategy, ByteBuffer data) {
    try {
      return compressorFactory.getCompressor(compressionStrategy).decompress(data);
    } catch (IOException e) {
      throw new VeniceClientException(
          String.format("Unable to decompress the record, compressionStrategy=%d", compressionStrategy.getValue()),
          e);
    }
  }

  @Override
  public void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    if (handleCallbackForEmptyKeySet(keys, callback)) {
      // empty key set
      return;
    }
    tryStart(1);

    List<K> keyList = new ArrayList<>(keys);
    TrackingStreamingCallback<K, V> decoderCallback = DelegatingTrackingCallback.wrap(callback);
    RecordStreamDecoder decoder = new MultiGetRecordStreamDecoder<>(
        keyList,
        decoderCallback,
        getDeserializationExecutor(),
        streamingFooterRecordDeserializer,
        this::getDataRecordDeserializerFromCache,
        this::decompressRecord);
    streamingBatchGet(keyList, decoder, decoderCallback.getStats());
  }

  private void streamingBatchGet(
      List<K> keyList,
      TransportClientStreamingCallback callback,
      Optional<ClientStats> stats) throws VeniceClientException {
    byte[] serializedRequest = serializeMultiGetRequest(keyList, stats);
    transportClient.streamPost(
        getStorageRequestPath(),
        RequestHeadersProvider.getThinClientStreamingBatchGetHeaders(keyList.size()),
        serializedRequest,
        callback,
        keyList.size());
  }

  private byte[] serializeMultiGetRequest(List<K> keyList, Optional<ClientStats> stats) {
    long startTime = System.nanoTime();
    List<ByteBuffer> serializedKeyList = new ArrayList<>(keyList.size());
    for (K key: keyList) {
      serializedKeyList.add(ByteBuffer.wrap(keySerializer.serialize(key)));
    }
    byte[] result = multiGetRequestSerializer.serializeObjects(serializedKeyList);
    stats.ifPresent(s -> s.recordRequestSerializationTime(LatencyUtils.getElapsedTimeFromNSToMS(startTime)));
    return result;
  }

  protected RecordDeserializer<V> getDataRecordDeserializerFromCache(int schemaId) {
    return deserializerCache.computeIfAbsent(schemaId, this::getDataRecordDeserializer);
  }

  protected static boolean handleCallbackForEmptyKeySet(Collection<?> keys, StreamingCallback callback) {
    if (keys.isEmpty()) {
      // no result for empty key set
      callback.onCompletion(Optional.empty());
      return true;
    }
    return false;
  }
}
