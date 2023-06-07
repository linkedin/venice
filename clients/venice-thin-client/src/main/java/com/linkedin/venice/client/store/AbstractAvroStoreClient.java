package com.linkedin.venice.client.store;

import static com.linkedin.venice.HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID;
import static com.linkedin.venice.HttpConstants.VENICE_KEY_COUNT;
import static com.linkedin.venice.VeniceConstants.COMPUTE_REQUEST_VERSION_V2;
import static com.linkedin.venice.streaming.StreamingConstants.KEY_ID_FOR_STREAMING_FOOTER;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.ServiceDiscoveryException;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.schema.RouterBackedSchemaReader;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.deserialization.BatchDeserializer;
import com.linkedin.venice.client.store.streaming.ComputeResponseRecordV1ChunkedDeserializer;
import com.linkedin.venice.client.store.streaming.MultiGetResponseRecordV1ChunkedDeserializer;
import com.linkedin.venice.client.store.streaming.ReadEnvelopeChunkedDeserializer;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.TrackingStreamingCallback;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.VeniceSerializationException;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.digests.MD5Digest;


public abstract class AbstractAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(AbstractAvroStoreClient.class);
  public static final String TYPE_STORAGE = "storage";
  public static final String TYPE_COMPUTE = "compute";
  public static final String B64_FORMAT = "?f=b64";

  private static final Map<String, String> GET_HEADER_MAP = new HashMap<>();
  private static final Map<String, String> MULTI_GET_HEADER_MAP = new HashMap<>();
  private static final Map<String, String> MULTI_GET_HEADER_MAP_FOR_STREAMING;
  private static final Map<String, String> COMPUTE_HEADER_MAP_V2 = new HashMap<>();
  private static final Map<String, String> COMPUTE_HEADER_MAP_V3 = new HashMap<>();
  static final Map<String, String> COMPUTE_HEADER_MAP_FOR_STREAMING_V2;
  static final Map<String, String> COMPUTE_HEADER_MAP_FOR_STREAMING_V3;

  static {
    /**
     * Hard-code API version of single-get and multi-get to be '1'.
     * If the header varies request by request, Venice client needs to create a map per request.
     */
    GET_HEADER_MAP.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_CLIENT_REQUEST_V1.getProtocolVersion()));
    GET_HEADER_MAP.put(
        HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY,
        Integer.toString(CompressionStrategy.GZIP.getValue()));

    MULTI_GET_HEADER_MAP.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getProtocolVersion()));
    MULTI_GET_HEADER_MAP.put(
        HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY,
        Integer.toString(CompressionStrategy.GZIP.getValue()));

    /**
     * COMPUTE_REQUEST_V1 is deprecated.
     */
    COMPUTE_HEADER_MAP_V2.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V2.getProtocolVersion()));

    COMPUTE_HEADER_MAP_V3.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V3.getProtocolVersion()));

    MULTI_GET_HEADER_MAP_FOR_STREAMING = new HashMap<>(MULTI_GET_HEADER_MAP);
    MULTI_GET_HEADER_MAP_FOR_STREAMING.put(HttpConstants.VENICE_STREAMING, "1");

    COMPUTE_HEADER_MAP_FOR_STREAMING_V2 = new HashMap<>(COMPUTE_HEADER_MAP_V2);
    COMPUTE_HEADER_MAP_FOR_STREAMING_V2.put(HttpConstants.VENICE_STREAMING, "1");

    COMPUTE_HEADER_MAP_FOR_STREAMING_V3 = new HashMap<>(COMPUTE_HEADER_MAP_V3);
    COMPUTE_HEADER_MAP_FOR_STREAMING_V3.put(HttpConstants.VENICE_STREAMING, "1");

    AvroVersion version = AvroCompatibilityHelperCommon.getRuntimeAvroVersion();
    LOGGER.info("Detected: {} on the classpath.", version);
  }

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
  private final BatchDeserializer<MultiGetResponseRecordV1, K, V> batchGetDeserializer;
  private final BatchDeserializer<ComputeResponseRecordV1, K, GenericRecord> computeDeserializer;
  private final CompressorFactory compressorFactory;
  private final String storageRequestPath;
  private final String computeRequestPath;

  private volatile boolean isServiceDiscovered;

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
  private static Executor DESERIALIZATION_EXECUTOR;

  private volatile boolean whetherStoreInitTriggeredByRequestFail = false;

  private Thread asyncStoreInitThread;
  private static final long ASYNC_STORE_INIT_SLEEP_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1); // 1ms
  private long asyncStoreInitSleepIntervalMs = ASYNC_STORE_INIT_SLEEP_INTERVAL_MS;

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
    this.batchGetDeserializer = clientConfig.getBatchGetDeserializer(this.deserializationExecutor);
    this.computeDeserializer = clientConfig.getBatchGetDeserializer(this.deserializationExecutor);
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

  protected SchemaReader getSchemaReader() {
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

  // For testing
  public void setAsyncStoreInitSleepIntervalMs(long intervalMs) {
    this.asyncStoreInitSleepIntervalMs = intervalMs;
  }

  /**
   * This function will try to initialize the store client at most once in a blocking fashion, and if the init
   * fails, one async thread will be kicked off to init the store client periodically until the init succeeds.
   */
  protected RecordSerializer<K> getKeySerializerForRequest() {
    if (keySerializer != null) {
      return keySerializer;
    }
    if (whetherStoreInitTriggeredByRequestFail) {
      // Store init already fails.
      throw new VeniceClientException("Failed to init store client for store: " + getStoreName());
    }
    synchronized (this) {
      try {
        if (keySerializer != null) {
          whetherStoreInitTriggeredByRequestFail = false;
          return keySerializer;
        }
        return getKeySerializerWithRetryWithShortInterval();
      } catch (Exception e) {
        whetherStoreInitTriggeredByRequestFail = true;
        // Kick off an async thread to keep retrying
        if (asyncStoreInitThread == null) {
          // Spin up at most one async thread
          asyncStoreInitThread = new Thread(() -> {
            while (true) {
              try {
                getKeySerializerWithRetryWithShortInterval();
                whetherStoreInitTriggeredByRequestFail = false;
                LOGGER.info("Successfully init store client by async store init thread");
                break;
              } catch (Exception ee) {
                if (ee instanceof InterruptedException || !LatencyUtils.sleep(asyncStoreInitSleepIntervalMs)) {
                  LOGGER.warn("Async store init thread got interrupted, will exit the loop");
                  break;
                }
                LOGGER.error(
                    "Received exception while trying to init store client asynchronously, will keep retrying",
                    ee);
              }
            }
          });
          asyncStoreInitThread.start();
        }
        throw e;
      }
    }
  }

  protected RecordSerializer<K> getKeySerializerWithoutRetry() {
    return getKeySerializerWithRetry(false, -1);
  }

  private RecordSerializer<K> getKeySerializerWithRetryWithShortInterval() {
    return getKeySerializerWithRetry(true, 50);
  }

  private RecordSerializer<K> getKeySerializerWithRetryWithLongInterval() {
    return getKeySerializerWithRetry(true, 1000);
  }

  private RecordSerializer<K> getKeySerializerWithRetry(boolean retryOnServiceDiscoveryFailure, int retryIntervalInMs) {
    if (keySerializer != null) {
      return keySerializer;
    }

    // Delay the dynamic d2 service discovery and key schema retrieval until it is necessary
    synchronized (this) {
      if (keySerializer != null) {
        return keySerializer;
      }

      Throwable lastException = null;
      int retryLimit = retryOnServiceDiscoveryFailure ? 10 : 1;
      for (int retryCount = 0; retryCount < retryLimit; ++retryCount) {
        if (retryCount > 0) {
          try {
            // Short sleep interval should be good enough, and we assume the next retry could hit a different Router.
            Thread.sleep(retryIntervalInMs);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new VeniceException("Initialization of Venice client is interrupted");
          }
        }
        try {
          init();
          return keySerializer;
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
      }
      throw new VeniceException("Failed to initializing Venice Client for store: " + getStoreName(), lastException);
    }
  }

  /**
   * During the initialization, we do the cluster discovery at first to find the real end point this client need to talk
   * to, before initializing the serializer.
   * So if sub-implementation need to have its own serializer, please override the initSerializer method.
   */
  protected void init() {
    discoverD2Service(false);
    initSerializer();
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

  protected void initSerializer() {
    // init key serializer
    if (needSchemaReader) {
      if (getSchemaReader() != null) {
        // init multi-get request serializer
        this.multiGetRequestSerializer = getClientConfig().isUseFastAvro()
            ? FastSerializerDeserializerFactory
                .getAvroGenericSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema())
            : SerializerDeserializerFactory
                .getAvroGenericSerializer(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
        // init compute request serializer
        this.computeRequestClientKeySerializer = getClientConfig().isUseFastAvro()
            ? FastSerializerDeserializerFactory
                .getAvroGenericSerializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema())
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
        this.keySerializer = getClientConfig().isUseFastAvro()
            ? FastSerializerDeserializerFactory.getAvroGenericSerializer(getSchemaReader().getKeySchema())
            : SerializerDeserializerFactory.getAvroGenericSerializer(getSchemaReader().getKeySchema());
      } else {
        throw new VeniceClientException("SchemaReader is null while initializing serializer");
      }
    }
  }

  // For testing
  public String getRequestPathByKey(K key) throws VeniceClientException {
    byte[] serializedKey = getKeySerializerForRequest().serialize(key);
    return getStorageRequestPathForSingleKey(serializedKey);
  }

  @Override
  public CompletableFuture<V> get(K key, Optional<ClientStats> stats, long preRequestTimeInNS)
      throws VeniceClientException {
    byte[] serializedKey = getKeySerializerForRequest().serialize(key);
    String requestPath = getStorageRequestPathForSingleKey(serializedKey);
    CompletableFuture<V> valueFuture = new CompletableFuture<>();

    requestSubmissionWithStatsHandling(
        stats,
        preRequestTimeInNS,
        true,
        () -> transportClient.get(requestPath, GET_HEADER_MAP),
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
                      .recordResponseDecompressionTime(LatencyUtils.getLatencyInMS(decompressionStartTime)));
              RecordDeserializer<V> deserializer = getDataRecordDeserializer(response.getSchemaId());
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
     * And we could not use {@link #getKeySchema()} since it will cause a dead loop:
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

  private byte[] serializeMultiGetRequest(List<K> keyList) {
    List<ByteBuffer> serializedKeyList = new ArrayList<>(keyList.size());
    RecordSerializer<K> keySerializer = getKeySerializerForRequest();
    for (K key: keyList) {
      serializedKeyList.add(ByteBuffer.wrap(keySerializer.serialize(key)));
    }
    return multiGetRequestSerializer.serializeObjects(serializedKeyList);
  }

  private <T> T tryToDeserialize(RecordDeserializer<T> dataDeserializer, ByteBuffer data, int writerSchemaId, K key) {
    return tryToDeserializeWithVerboseLogging(
        dataDeserializer,
        data,
        writerSchemaId,
        key,
        getKeySerializerForRequest(),
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
      String checksumHex, keyHex, latestSchemaId;
      try {
        // Hashing the value because 1) values tend to be too large for logs and 2) they might contain PII
        MD5Digest digest = new MD5Digest();
        byte[] valueChecksum = new byte[digest.getDigestSize()];
        digest.update(data.array(), data.position(), data.limit() - data.position());
        digest.doFinal(valueChecksum, 0);
        checksumHex = Hex.encodeHexString(valueChecksum);
      } catch (Exception e2) {
        checksumHex = "failed to compute value checksum";
        LOGGER.error("{} ...", checksumHex, e2);
      }

      try {
        keyHex = Hex.encodeHexString(keySerializer.serialize(key));
      } catch (Exception e3) {
        keyHex = "failed to serialize key and encode it as hex";
        LOGGER.error("{} ...", keyHex, e3);
      }

      try {
        latestSchemaId = schemaReader.getLatestValueSchemaId().toString();
      } catch (Exception e4) {
        latestSchemaId = "failed to retrieve latest value schema ID";
        LOGGER.error("{} ...", latestSchemaId, e4);
      }

      LOGGER.error(
          "Caught a {}, will bubble up."
              + "RecordDeserializer: {}\nWriter schema ID: {}\nLatest schema ID: {}\nValue (md5/hex): {}\nKey (hex): {}",
          VeniceSerializationException.class.getSimpleName(),
          dataDeserializer.getClass().getSimpleName(),
          (writerSchemaId == -1 ? "N/A" : writerSchemaId),
          latestSchemaId,
          checksumHex,
          keyHex);
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
          clientStats -> clientStats.recordRequestSerializationTime(
              LatencyUtils.convertLatencyFromNSToMS(preSubmitTimeInNS - preRequestTimeInNS)));
      stats.ifPresent(
          clientStats -> clientStats.recordRequestSubmissionToResponseHandlingTime(
              LatencyUtils.convertLatencyFromNSToMS(preHandlingTimeNS - preSubmitTimeInNS)));

      return responseHandler.handle(
          clientResponse,
          throwable,
          () -> stats.ifPresent(
              clientStats -> clientStats
                  .recordResponseDeserializationTime(LatencyUtils.getLatencyInMS(preHandlingTimeNS))));
    };

    if (handleResponseOnDeserializationExecutor) {
      return transportFuture.handleAsync(responseHandlerWithStats, deserializationExecutor);
    } else {
      return transportFuture.handle(responseHandlerWithStats);
    }
  }

  private interface ResponseHandler<R> {
    R handle(TransportClientResponse clientResponse, Throwable throwable, Reporter responseDeserializationComplete);
  }

  @Override
  public ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      long preRequestTimeInNS) {
    return compute(stats, streamingStats, this, preRequestTimeInNS);
  }

  @Override
  public ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      InternalAvroStoreClient computeStoreClient,
      long preRequestTimeInNS) {
    return new AvroComputeRequestBuilderV3<K>(computeStoreClient, getLatestValueSchema()).setStats(streamingStats)
        .setValidateProjectionFields(getClientConfig().isProjectionFieldValidationEnabled());
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    if (handleCallbackForEmptyKeySet(keys, callback)) {
      // empty key set
      return;
    }

    Optional<ClientStats> clientStats = Optional.empty();
    if (callback instanceof TrackingStreamingCallback) {
      clientStats = Optional.of(((TrackingStreamingCallback<K, V>) callback).getStats());
    }

    List<K> keyList = new ArrayList<>(keys);
    long preRequestSerializationNanos = System.nanoTime();
    byte[] serializedComputeRequest = serializeComputeRequest(computeRequestWrapper, keyList);
    clientStats.ifPresent(
        stats -> stats.recordRequestSerializationTime(LatencyUtils.getLatencyInMS(preRequestSerializationNanos)));

    int schemaId = getSchemaReader().getValueSchemaId(computeRequestWrapper.getValueSchema());
    Map<String, String> headerMap = new HashMap<>(
        (computeRequestWrapper.getComputeRequestVersion() == COMPUTE_REQUEST_VERSION_V2)
            ? COMPUTE_HEADER_MAP_FOR_STREAMING_V2
            : COMPUTE_HEADER_MAP_FOR_STREAMING_V3);
    headerMap.put(VENICE_KEY_COUNT, Integer.toString(keyList.size()));
    headerMap.put(VENICE_COMPUTE_VALUE_SCHEMA_ID, Integer.toString(schemaId));

    RecordDeserializer<GenericRecord> computeResultRecordDeserializer =
        getComputeResultRecordDeserializer(resultSchema);

    transportClient.streamPost(
        getComputeRequestPath(),
        headerMap,
        serializedComputeRequest,
        new StoreClientStreamingCallback<>(keyList, callback, envelopeSchemaId -> {
          validateComputeResponseSchemaId(envelopeSchemaId);
          return new ComputeResponseRecordV1ChunkedDeserializer();
        },
            // Compute doesn't support compression
            (envelope, compressionStrategy) -> {
              if (!envelope.value.hasRemaining()) {
                // Safeguard to handle empty value, which indicates non-existing key.
                return null;
              }
              return new ComputeGenericRecord(
                  computeResultRecordDeserializer.deserialize(envelope.value),
                  computeRequestWrapper.getValueSchema());
            },
            envelope -> envelope.keyIndex,
            envelope -> streamingFooterRecordDeserializer.deserialize(envelope.value)),
        keyList.size());
  }

  private byte[] serializeComputeRequest(ComputeRequestWrapper computeRequestWrapper, Collection<K> keys) {
    RecordSerializer keySerializer = getKeySerializerForRequest();
    List<ByteBuffer> serializedKeyList = new ArrayList<>(keys.size());
    ByteBuffer serializedComputeRequest = ByteBuffer.wrap(computeRequestWrapper.serialize());
    for (K key: keys) {
      serializedKeyList.add(ByteBuffer.wrap(keySerializer.serialize(key)));
    }
    return computeRequestClientKeySerializer.serializeObjects(serializedKeyList, serializedComputeRequest);
  }

  @Override
  public void start() throws VeniceClientException {
    if (needSchemaReader) {
      this.schemaReader = new RouterBackedSchemaReader(
          this,
          getReaderSchema(),
          clientConfig.getPreferredSchemaFilter(),
          clientConfig.getSchemaRefreshPeriod(),
          null);
    }
    warmUpVeniceClient();
  }

  /**
   * The behavior of READ apis will be non-deterministic after `close` function is called.
   */
  @Override
  public void close() {
    IOUtils.closeQuietly(transportClient, LOGGER::error);
    IOUtils.closeQuietly(schemaReader, LOGGER::error);
    IOUtils.closeQuietly(compressorFactory, LOGGER::error);
    if (asyncStoreInitThread != null) {
      asyncStoreInitThread.interrupt();
    }
  }

  protected Optional<Schema> getReaderSchema() {
    return Optional.empty();
  }

  public abstract RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException;

  private void warmUpVeniceClient() {
    if (getClientConfig().isForceClusterDiscoveryAtStartTime()) {
      /**
       * Force the client initialization and fail fast if any error happens.
       */
      getKeySerializerWithRetryWithShortInterval();
    } else {
      /**
       * Try to warm-up the Venice Client during start phase, and it may not work since it is possible that the passed d2
       * client hasn't been fully started yet, when this happens, the warm-up will be delayed to the first query.
       */
      try {
        getKeySerializerWithoutRetry();
      } catch (Exception e) {
        LOGGER.info(
            "Got error when trying to warm up client during start phase for store: {}, and will kick off an "
                + "async warm-up:{}",
            getStoreName(),
            e.getMessage());
        /**
         * Kick off an async warm-up, and the D2 client could be ready during the async warm-up.
         * If the D2 client isn't retry in the async warm-up phase, it will be delayed to the first query.
         * Essentially, this is a best-effort.
         */
        CompletableFuture.runAsync(() -> getKeySerializerWithRetryWithLongInterval());
      }
    }
  }

  private void validateMultiGetResponseSchemaId(int schemaId) {
    int protocolVersion = ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
    if (protocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + protocolVersion);
    }
  }

  private void validateComputeResponseSchemaId(int schemaId) {
    int protocolVersion = ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion();
    if (protocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + protocolVersion);
    }
  }

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

  private interface DeserializerFunc<ENVELOPE, V> {
    V deserialize(ENVELOPE envelope, CompressionStrategy compressionStrategy);
  }

  /**
   * Streaming callback for batch-get/compute.
   *
   * Since data chunk returned by {@link D2TransportClient} doesn't respect chunk-size associated with each
   * chunk sent by Venice Router, here is leveraging {@link ReadEnvelopeChunkedDeserializer} to deserialize
   * data chunks even with the data chunk contains partial record in a non-blocking way.
   *
   * The envelope deserialization will happen in TransportClient thread pool (R2 thread pool for example if using
   * {@link D2TransportClient}, and both the record deserialization and application's callback will be executed in
   * Venice thread pool: {@link #deserializationExecutor},
   *
   * @param <ENVELOPE>
   * @param <K>
   * @param <V>
   */
  private class StoreClientStreamingCallback<ENVELOPE, K, V> implements TransportClientStreamingCallback {
    private final List<K> keyList;
    private final StreamingCallback<K, V> callback;
    private final Function<Integer, ReadEnvelopeChunkedDeserializer<ENVELOPE>> envelopeDeserializerFunc;
    private final DeserializerFunc<ENVELOPE, V> recordDeserializerFunc;
    private final Function<ENVELOPE, Integer> indexRetrievalFunc;
    private final Function<ENVELOPE, StreamingFooterRecordV1> streamingFooterRecordDeserializer;

    private boolean isStreamingResponse = false;
    private int responseSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
    private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
    private ReadEnvelopeChunkedDeserializer<ENVELOPE> envelopeDeserializer = null;
    private List<CompletableFuture<Void>> deserializationFutures = new ArrayList<>();
    // Track which key is present in the response
    private final BitSet receivedKeySet;
    private final AtomicInteger successfulKeyCnt = new AtomicInteger(0);
    private int duplicateEntryCnt = 0;
    private Optional<StreamingFooterRecordV1> streamingFooterRecord = Optional.empty();
    private Optional<TrackingStreamingCallback> trackingStreamingCallback = Optional.empty();
    private final long preSubmitTimeInNS;
    private final Optional<ClientStats> clientStats;
    private final LongAdder deserializationTimeInNS = new LongAdder();

    public StoreClientStreamingCallback(
        List<K> keyList,
        StreamingCallback<K, V> callback,
        Function<Integer, ReadEnvelopeChunkedDeserializer<ENVELOPE>> envelopeDeserializerFunc,
        DeserializerFunc<ENVELOPE, V> recordDeserializerFunc,
        Function<ENVELOPE, Integer> indexRetrievalFunc,
        Function<ENVELOPE, StreamingFooterRecordV1> streamingFooterRecordDeserializer) {
      this.keyList = keyList;
      this.callback = callback;
      this.preSubmitTimeInNS = System.nanoTime();
      if (callback instanceof TrackingStreamingCallback) {
        trackingStreamingCallback = Optional.of((TrackingStreamingCallback) callback);
        this.clientStats = Optional.of(trackingStreamingCallback.get().getStats());
      } else {
        this.clientStats = Optional.empty();
      }
      this.envelopeDeserializerFunc = envelopeDeserializerFunc;
      this.recordDeserializerFunc = recordDeserializerFunc;
      this.indexRetrievalFunc = indexRetrievalFunc;
      this.receivedKeySet = new BitSet(keyList.size());
      this.streamingFooterRecordDeserializer = streamingFooterRecordDeserializer;
    }

    @Override
    public void onHeaderReceived(Map<String, String> headers) {
      clientStats.ifPresent(
          stats -> stats.recordRequestSubmissionToResponseHandlingTime(LatencyUtils.getLatencyInMS(preSubmitTimeInNS)));
      isStreamingResponse = headers.containsKey(HttpConstants.VENICE_STREAMING_RESPONSE);
      String schemaIdHeader = headers.get(HttpConstants.VENICE_SCHEMA_ID);
      if (schemaIdHeader != null) {
        responseSchemaId = Integer.parseInt(schemaIdHeader);
      }
      envelopeDeserializer = envelopeDeserializerFunc.apply(responseSchemaId);
      String compressionHeader = headers.get(HttpConstants.VENICE_COMPRESSION_STRATEGY);
      if (compressionHeader != null) {
        compressionStrategy = CompressionStrategy.valueOf(Integer.parseInt(compressionHeader));
      }
    }

    private void validateKeyIdx(int keyIdx) {
      if (KEY_ID_FOR_STREAMING_FOOTER == keyIdx) {
        // footer record
        return;
      }
      final int absKeyIdx = Math.abs(keyIdx);
      if (absKeyIdx < keyList.size()) {
        return;
      }
      throw new VeniceClientException(
          "Invalid key index: " + keyIdx + ", either it should be the footer" + " record key index: "
              + KEY_ID_FOR_STREAMING_FOOTER + " or its absolute value should be [0, " + keyList.size() + ")");
    }

    @Override
    public void onDataReceived(ByteBuffer chunk) {
      if (envelopeDeserializer == null) {
        throw new VeniceClientException("Envelope deserializer hasn't been initialized yet");
      }
      envelopeDeserializer.write(chunk);
      // Envelope deserialization has to happen sequentially
      final List<ENVELOPE> availableRecords = envelopeDeserializer.consume();
      if (availableRecords.isEmpty()) {
        // no full record is available
        return;
      }
      CompletableFuture<Void> deserializationFuture = CompletableFuture.runAsync(() -> {
        Map<K, V> resultMap = new HashMap<>();
        for (ENVELOPE record: availableRecords) {
          final int keyIdx = indexRetrievalFunc.apply(record);
          validateKeyIdx(keyIdx);
          if (KEY_ID_FOR_STREAMING_FOOTER == keyIdx) {
            // Deserialize footer record
            streamingFooterRecord = Optional.of(streamingFooterRecordDeserializer.apply(record));
            break;
          }
          final int absKeyIdx = Math.abs(keyIdx);
          // Track duplicate entries per request
          if (absKeyIdx < keyList.size()) {
            synchronized (receivedKeySet) {
              if (receivedKeySet.get(absKeyIdx)) {
                // Encounter duplicate entry because of retrying logic in Venice Router
                ++duplicateEntryCnt;
                continue;
              }
              receivedKeySet.set(absKeyIdx);
            }
          }
          K key = keyList.get(absKeyIdx);

          V value;
          if (keyIdx < 0) {
            // Key doesn't exist
            value = null;
          } else {
            /**
             * The above condition could NOT capture the non-existing key with index: 0,
             * so {@link DeserializerFunc#deserialize(Object, CompressionStrategy)} needs to handle it by checking
             * whether the value is an empty byte array or not, and essentially the deserialization function should
             * return null in this situation.
             */
            long preRecordDeserializationInNS = System.nanoTime();
            value = recordDeserializerFunc.deserialize(record, compressionStrategy);
            deserializationTimeInNS.add(System.nanoTime() - preRecordDeserializationInNS);
            /**
             * If key index is not 0, it is unexpected to receive non-null value.
             */
            if (value == null && keyIdx != 0) {
              throw new VeniceClientException("Expected to receive non-null value for key: " + keyList.get(keyIdx));
            }
          }
          trackingStreamingCallback.ifPresent(t -> t.onRecordDeserialized());
          resultMap.put(key, value);
          if (value != null) {
            successfulKeyCnt.incrementAndGet();
          }
        }
        if (resultMap.isEmpty()) {
          return;
        }
        /**
         * Execute the user callback in the same thread.
         *
         * There is a bug in JDK8, which could cause {@link CompletableFuture#allOf(CompletableFuture[])} if there
         * are multiple layers of async processing:
         * https://bugs.openjdk.java.net/browse/JDK-8201576
         * So if the user's callback is executed in another async handler, {@link CompletableFuture#allOf(CompletableFuture[])}
         * will hang sometimes.
         * Also with this way, the context switches are also reduced.
          */
        resultMap.forEach((k, v) -> callback.onRecordReceived(k, v));
      }, deserializationExecutor);
      deserializationFutures.add(deserializationFuture);
    }

    @Override
    public void onCompletion(Optional<VeniceClientException> exception) {
      // Only complete it when all the futures are done.
      CompletableFuture.allOf(deserializationFutures.toArray(new CompletableFuture[deserializationFutures.size()]))
          .whenComplete((voidP, throwable) -> {
            Optional<Exception> completedException = Optional.empty();
            if (exception.isPresent()) {
              // Exception thrown by transporting layer
              completedException = Optional.of(exception.get());
            } else if (streamingFooterRecord.isPresent()) {
              // Exception thrown by Venice backend
              completedException = Optional.of(
                  new VeniceClientHttpException(
                      new String(ByteUtils.extractByteArray(streamingFooterRecord.get().detail)),
                      streamingFooterRecord.get().status));
            } else if (throwable != null) {
              if (throwable instanceof Exception) {
                completedException = Optional.of((Exception) throwable);
              } else {
                completedException = Optional.of(new Exception(throwable));
              }
            } else {
              // Everything is good
              if (!isStreamingResponse) {
                /**
                 * For regular response, the non-existing keys won't be present in the response,
                 * so we need to manually collect the non-existing keys and trigger customer's callback.
                 */
                for (int i = 0; i < keyList.size(); ++i) {
                  if (!receivedKeySet.get(i)) {
                    callback.onRecordReceived(keyList.get(i), null);
                    trackingStreamingCallback.ifPresent(t -> t.onRecordDeserialized());
                    /**
                     * To match the streaming response, we will mark the non-existing keys as received as well.
                     */
                    receivedKeySet.set(i);
                  }
                }
              }
            }
            callback.onCompletion(completedException);
            final Optional<Exception> finalCompletedException = completedException;
            trackingStreamingCallback.ifPresent(
                t -> t.onDeserializationCompletion(finalCompletedException, successfulKeyCnt.get(), duplicateEntryCnt));
            clientStats.ifPresent(
                stats -> stats.recordResponseDeserializationTime(
                    LatencyUtils.convertLatencyFromNSToMS(deserializationTimeInNS.sum())));
          });
    }
  }

  protected boolean handleCallbackForEmptyKeySet(Set<K> keys, StreamingCallback callback) {
    if (keys.isEmpty()) {
      // no result for empty key set
      callback.onCompletion(Optional.empty());
      return true;
    }
    return false;
  }

  @Override
  public void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    if (handleCallbackForEmptyKeySet(keys, callback)) {
      // empty key set
      return;
    }
    Optional<ClientStats> clientStats = Optional.empty();
    if (callback instanceof TrackingStreamingCallback) {
      clientStats = Optional.of(((TrackingStreamingCallback<K, V>) callback).getStats());
    }

    List<K> keyList = new ArrayList<>(keys);
    long preRequestSerializationNS = System.nanoTime();
    byte[] multiGetBody = serializeMultiGetRequest(keyList);
    clientStats.ifPresent(
        stats -> stats.recordRequestSerializationTime(LatencyUtils.getLatencyInMS(preRequestSerializationNS)));

    Map<Integer, RecordDeserializer<V>> deserializerCache = new VeniceConcurrentHashMap<>();
    Map<String, String> headerMap = new HashMap<>(MULTI_GET_HEADER_MAP_FOR_STREAMING);
    headerMap.put(VENICE_KEY_COUNT, Integer.toString(keyList.size()));

    transportClient.streamPost(
        getStorageRequestPath(),
        headerMap,
        multiGetBody,
        new StoreClientStreamingCallback<>(keyList, callback, envelopeSchemaId -> {
          validateMultiGetResponseSchemaId(envelopeSchemaId);
          return new MultiGetResponseRecordV1ChunkedDeserializer();
        }, (envelope, compressionStrategy) -> {
          if (!envelope.value.hasRemaining()) {
            // Safeguard to handle empty value, which indicates non-existing key.
            return null;
          }
          RecordDeserializer<V> recordDeserializer =
              deserializerCache.computeIfAbsent(envelope.schemaId, id -> getDataRecordDeserializer(id));
          ByteBuffer decompressedValue = decompressRecord(compressionStrategy, envelope.value);
          return recordDeserializer.deserialize(decompressedValue);
        }, envelope -> envelope.keyIndex, envelope -> streamingFooterRecordDeserializer.deserialize(envelope.value)),
        keyList.size());
  }
}
