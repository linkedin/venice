package com.linkedin.venice.client.store;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.stats.Reporter;
import com.linkedin.venice.client.store.deserialization.BatchDeserializer;
import com.linkedin.venice.client.store.streaming.ComputeResponseRecordV1ChunkedDeserializer;
import com.linkedin.venice.client.store.streaming.MultiGetResponseRecordV1ChunkedDeserializer;
import com.linkedin.venice.client.store.streaming.ReadEnvelopeChunkedDeserializer;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.TrackingStreamingCallback;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.CompressionStrategy;
import io.tehuti.utils.Time;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.validation.constraints.NotNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.AvroVersion;
import org.apache.avro.io.ByteBufferOptimizedBinaryDecoder;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.streaming.StreamingConstants.*;


public abstract class AbstractAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private static Logger logger = Logger.getLogger(AbstractAvroStoreClient.class);
  public static final String TYPE_STORAGE = "storage";
  public static final String TYPE_COMPUTE = "compute";
  public static final String B64_FORMAT = "?f=b64";

  private static final Map<String, String> GET_HEADER_MAP = new HashMap<>();
  private static final Map<String, String> MULTI_GET_HEADER_MAP = new HashMap<>();
  private static final Map<String, String> MULTI_GET_HEADER_MAP_FOR_STREAMING;
  private static final Map<String, String> COMPUTE_HEADER_MAP_V2 = new HashMap<>();
  private static final Map<String, String> COMPUTE_HEADER_MAP_V1 = new HashMap<>();
  private static final Map<String, String> COMPUTE_HEADER_MAP_FOR_STREAMING_V2;
  private static final Map<String, String> COMPUTE_HEADER_MAP_FOR_STREAMING_V1;

  static {
    /**
     * Hard-code API version of single-get and multi-get to be '1'.
     * If the header varies request by request, Venice client needs to create a map per request.
     */
    GET_HEADER_MAP.put(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_CLIENT_REQUEST_V1.getProtocolVersion()));
    GET_HEADER_MAP.put(HttpConstants.VENICE_SUPPORTED_COMPRESSION,
        Integer.toString(CompressionStrategy.GZIP.getValue()));

    MULTI_GET_HEADER_MAP.put(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getProtocolVersion()));
    MULTI_GET_HEADER_MAP.put(HttpConstants.VENICE_SUPPORTED_COMPRESSION,
        Integer.toString(CompressionStrategy.GZIP.getValue()));

    COMPUTE_HEADER_MAP_V2.put(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V2.getProtocolVersion()));

    COMPUTE_HEADER_MAP_V1.put(HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V1.getProtocolVersion()));

    MULTI_GET_HEADER_MAP_FOR_STREAMING = new HashMap<>(MULTI_GET_HEADER_MAP);
    MULTI_GET_HEADER_MAP_FOR_STREAMING.put(HttpConstants.VENICE_STREAMING, "1");

    COMPUTE_HEADER_MAP_FOR_STREAMING_V2 = new HashMap<>(COMPUTE_HEADER_MAP_V2);
    COMPUTE_HEADER_MAP_FOR_STREAMING_V2.put(HttpConstants.VENICE_STREAMING, "1");

    COMPUTE_HEADER_MAP_FOR_STREAMING_V1 = new HashMap<>(COMPUTE_HEADER_MAP_V1);
    COMPUTE_HEADER_MAP_FOR_STREAMING_V1.put(HttpConstants.VENICE_STREAMING, "1");

    AvroVersion version = LinkedinAvroMigrationHelper.getRuntimeAvroVersion();
    logger.info("Detected: " + version.toString() + " on the classpath.");
  }

  private final CompletableFuture<Map<K, V>> COMPLETABLE_FUTURE_FOR_EMPTY_KEY_IN_BATCH_GET = CompletableFuture.completedFuture(new HashMap<>());
  private final CompletableFuture<Map<K, GenericRecord>> COMPLETABLE_FUTURE_FOR_EMPTY_KEY_IN_COMPUTE = CompletableFuture.completedFuture(new HashMap<>());

  private final Boolean needSchemaReader;
  /** Used to communicate with Venice backend to retrieve necessary store schemas */
  private SchemaReader schemaReader;
  // Key serializer
  protected RecordSerializer<K> keySerializer = null;
  // Multi-get request serializer
  protected RecordSerializer<ByteBuffer> multiGetRequestSerializer;
  protected RecordSerializer<ByteBuffer> computeRequestClientKeySerializer;

  private RecordDeserializer<StreamingFooterRecordV1> streamingFooterRecordDeserializer;

  private TransportClient transportClient;
  private String storeName;
  private D2ServiceDiscovery d2ServiceDiscovery = new D2ServiceDiscovery();
  private final Executor deserializationExecutor;
  private final BatchDeserializer<MultiGetResponseRecordV1, K, V> batchGetDeserializer;
  private final BatchDeserializer<ComputeResponseRecordV1, K, GenericRecord> computeDeserializer;
  private final AvroGenericDeserializer.IterableImpl multiGetEnvelopeIterableImpl;
  private final Time time;

  private final boolean useFastAvro;
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
  private static Executor DESERIALIZATION_EXECUTOR = null;

  public static synchronized Executor getDefaultDeserializationExecutor() {
    if (DESERIALIZATION_EXECUTOR == null) {
      // Half of process number of threads should be good enough, minimum 2
      int threadNum = Math.max(Runtime.getRuntime().availableProcessors() / 2, 2);

      DESERIALIZATION_EXECUTOR = Executors.newFixedThreadPool(threadNum,
          new DaemonThreadFactory("Venice-Store-Deserialization"));
    }

    return DESERIALIZATION_EXECUTOR;
  }

  protected AbstractAvroStoreClient(TransportClient transportClient, boolean needSchemaReader, ClientConfig clientConfig) {
    this.transportClient = transportClient;
    this.storeName = clientConfig.getStoreName();
    this.needSchemaReader = needSchemaReader;
    this.deserializationExecutor = Optional.ofNullable(clientConfig.getDeserializationExecutor())
        .orElse(getDefaultDeserializationExecutor());
    this.batchGetDeserializer = clientConfig.getBatchGetDeserializer(this.deserializationExecutor);
    this.computeDeserializer = clientConfig.getBatchGetDeserializer(this.deserializationExecutor);
    this.multiGetEnvelopeIterableImpl = clientConfig.getMultiGetEnvelopeIterableImpl();
    this.useFastAvro = clientConfig.isUseFastAvro();
    this.time = clientConfig.getTime();
  }

  protected boolean isUseFastAvro() {
    return useFastAvro;
  }

  @Override
  public String getStoreName() {
    return storeName;
  }

  protected TransportClient getTransportClient() {
    return transportClient;
  }

  @NotNull
  protected SchemaReader getSchemaReader() {
    return schemaReader;
  }

  @Override
  public Executor getDeserializationExecutor() {
    return deserializationExecutor;
  }

  private String getStorageRequestPathForSingleKey(byte[] key) {
    String b64key = EncodingUtils.base64EncodeToString(key);
    return getStorageRequestPath() +
        "/" + b64key + B64_FORMAT;
  }

  private String getStorageRequestPath() {
    return TYPE_STORAGE + "/" + storeName;
  }

  private String getComputeRequestPath() {
    return TYPE_COMPUTE + "/" + storeName;
  }

  protected RecordSerializer<K> getKeySerializer() {
    if (null != keySerializer) {
      return keySerializer;
    }
    // Delay the dynamic d2 service discovery and key schema retrieval until it is necessary
    synchronized (this) {
      if (null != keySerializer) {
        return keySerializer;
      }

      init();

      return keySerializer;
    }
  }

  /**
   * During the initialization, we do the cluster discovery at first to find the real end point this client need to talk
   * to, before initializing the serializer.
   * So if sub-implementation need to have its own serializer, please override the initSerializer method.
   */
  protected void init() {
    // Discover the proper d2 service name for this store.
    if(transportClient instanceof  D2TransportClient) {
      // Use the new d2 transport client which will talk to the cluster own the given store.
      // Do not need to close the original one, because if we use global d2 client, close will do nothing. If we use
      // private d2, we could not close it as we share this d2 client in the new transport client.
      transportClient = d2ServiceDiscovery.getD2TransportClientForStore((D2TransportClient) transportClient, storeName);
    }
    initSerializer();
  }

  protected void initSerializer() {
    // init key serializer
    this.keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(getSchemaReader().getKeySchema());
    // init multi-get request serializer
    this.multiGetRequestSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(
        ReadAvroProtocolDefinition.MULTI_GET_CLIENT_REQUEST_V1.getSchema());
    // init compute request serializer
    this.computeRequestClientKeySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(
        ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema());

    if (useFastAvro) {
      this.streamingFooterRecordDeserializer =
          FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(StreamingFooterRecordV1.SCHEMA$, StreamingFooterRecordV1.class);
    } else {
      this.streamingFooterRecordDeserializer =
          SerializerDeserializerFactory.getAvroSpecificDeserializer(StreamingFooterRecordV1.SCHEMA$, StreamingFooterRecordV1.class);
    }

  }

  // For testing
  public String getRequestPathByKey(K key) throws VeniceClientException {
    byte[] serializedKey = getKeySerializer().serialize(key);
    return getStorageRequestPathForSingleKey(serializedKey);
  }

  @Override
  public CompletableFuture<V> get(final K key, final Optional<ClientStats> stats, final long preRequestTimeInNS) throws VeniceClientException {
    byte[] serializedKey = getKeySerializer().serialize(key);
    String requestPath = getStorageRequestPathForSingleKey(serializedKey);

    return requestSubmissionWithStatsHandling(stats, preRequestTimeInNS, true,
        () -> transportClient.get(requestPath, GET_HEADER_MAP),
        (response, throwable) -> {
          if (null != throwable) {
            handleStoreExceptionInternally(throwable);
          }
          if (null == response) {
            // Doesn't exist
            return null;
          }
          if (!response.isSchemaIdValid()) {
            throw new VeniceClientException("No valid schema id received for single-get request!");
          }

          CompressionStrategy compressionStrategy = response.getCompressionStrategy();
          long decompressionStartTime = System.nanoTime();
          ByteBuffer data = decompressRecord(compressionStrategy, ByteBuffer.wrap(response.getBody()));
          stats.ifPresent((clientStats) ->
            clientStats.recordResponseDecompressionTime(LatencyUtils.getLatencyInMS(decompressionStartTime))
          );
          RecordDeserializer<V> deserializer = getDataRecordDeserializer(response.getSchemaId());
          return deserializer.deserialize(data.array());
        }
    );
  }

  @Override
  public CompletableFuture<byte[]> getRaw(String requestPath, Optional<ClientStats> stats, final long preRequestTimeInNS) {
    return requestSubmissionWithStatsHandling(stats, preRequestTimeInNS, false,
        () -> transportClient.get(requestPath),
        (clientResponse, throwable) -> {
          if (null != throwable) {
            handleStoreExceptionInternally(throwable);
          }
          if (null == clientResponse) {
            // Doesn't exist
            return null;
          }
          return clientResponse.getBody();
        }
    );
  }

  private byte[] serializeMultiGetRequest(List<K> keyList) {
    List<ByteBuffer> serializedKeyList = new ArrayList<>();
    keyList.stream().forEach(key -> serializedKeyList.add(ByteBuffer.wrap(getKeySerializer().serialize(key))));
    return multiGetRequestSerializer.serializeObjects(serializedKeyList);
  }

  @Override
  public CompletableFuture<Map<K, V>> batchGet(final Set<K> keys, final Optional<ClientStats> stats, final long preRequestTimeInNS) throws VeniceClientException {
    if (keys.isEmpty()) {
      return COMPLETABLE_FUTURE_FOR_EMPTY_KEY_IN_BATCH_GET;
    }
    List<K> keyList = new ArrayList<>(keys);
    LongAdder decompressionTime = new LongAdder();
    byte[] multiGetBody = serializeMultiGetRequest(keyList);
    CompletableFuture<Map<K, V>> valueFuture = new CompletableFuture();

    requestSubmissionWithStatsHandling(
        stats,
        preRequestTimeInNS,
        true,
        () -> transportClient.post(getStorageRequestPath(), MULTI_GET_HEADER_MAP, multiGetBody),
        (response, throwable, responseDeserializationComplete) -> {
          try {
            if (null != throwable) {
              // Error response, such as 400 or 5xx will be handled here.
              valueFuture.completeExceptionally(throwable);
            } else if (null == response) {
              // Even all the keys don't exist in Venice, multi-get should receive empty result instead of 'null'
              valueFuture.completeExceptionally(new VeniceClientException("TransportClient should not return null for multi-get request"));
            } else if (!response.isSchemaIdValid()) {
              valueFuture.completeExceptionally(new VeniceClientException("No valid schema id received for multi-get request!"));
            } else {
              long preResponseEnvelopeDeserialization = System.nanoTime();
              CompressionStrategy compressionStrategy = response.getCompressionStrategy();
              RecordDeserializer<MultiGetResponseRecordV1> deserializer = getMultiGetResponseRecordDeserializer(response.getSchemaId());
              Iterable<MultiGetResponseRecordV1> records = deserializer.deserializeObjects(new ByteBufferOptimizedBinaryDecoder(response.getBody()));
              /**
               * We cache the deserializers by schema ID in order to avoid the unnecessary lookup in
               * {@link SerializerDeserializerFactory}, which introduces small performance issue with
               * {@link Schema#hashCode} in avro-1.4, which is doing hash code calculation every time.
               *
               * It is possible that multiple deserializer threads could try to access the cache,
               * so we use {@link VeniceConcurrentHashMap}.
               */
              Map<Integer, RecordDeserializer<V>> deserializerCache = new VeniceConcurrentHashMap<>();
              batchGetDeserializer.deserialize(valueFuture, records, keyList, (resultMap, record) -> {
                int keyIndex = record.keyIndex;
                if (keyIndex >= keyList.size() || keyIndex < 0) {
                  valueFuture.completeExceptionally(new VeniceClientException("Key index: " + keyIndex + " doesn't have a corresponding key"));
                }
                RecordDeserializer<V> dataDeserializer = deserializerCache.computeIfAbsent(record.schemaId, id -> getDataRecordDeserializer(id));
                long decompressionStartTime = System.nanoTime();
                ByteBuffer data = decompressRecord(compressionStrategy, record.value);
                decompressionTime.add(System.nanoTime() - decompressionStartTime);
                resultMap.put(keyList.get(keyIndex), dataDeserializer.deserialize(data));
              }, responseDeserializationComplete, stats, preResponseEnvelopeDeserialization);
            }
          } catch (Exception e) {
            if (! valueFuture.isDone()) {
              valueFuture.completeExceptionally(e);
            } else {
              // Defensive code
              logger.error("Received exception after completing the `valueFuture` for batch-get request", e);
              throw e;
            }
          }
          return null;
        });

    stats.ifPresent((clientStats) -> valueFuture.thenRun(() ->
      clientStats.recordResponseDecompressionTime(LatencyUtils.convertLatencyFromNSToMS(decompressionTime.sum()))
    ));

    return valueFuture;
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
   * @param handleResponseOnDeserializationExecutor if true, will execute the {@param responseHandler} on the {@link #deserializationExecutor}
   *                                                if false, will execute the {@param responseHandler} on the same thread.
   * @param requestSubmitter A closure which ONLY submits the request to the backend. Should not include any pre-submission work (i.e.: serialization).
   * @param responseHandler A closure which interprets the response from the backend (i.e.: deserialization).
   * @param <R> The return type of the {@param responseHandler}.
   * @return a {@link CompletableFuture<R>} wrapping the return of the {@param responseHandler}.
   * @throws VeniceClientException
   */
  private <R> CompletableFuture<R> requestSubmissionWithStatsHandling(
      final Optional<ClientStats> stats,
      final long preRequestTimeInNS,
      final boolean handleResponseOnDeserializationExecutor,
      final Supplier<CompletableFuture<TransportClientResponse>> requestSubmitter,
      final ResponseHandler<R> responseHandler) throws VeniceClientException {
    final long preSubmitTimeInNS = System.nanoTime();
    CompletableFuture<TransportClientResponse> transportFuture = requestSubmitter.get();

    BiFunction<TransportClientResponse, Throwable, R> responseHandlerWithStats = (clientResponse, throwable) -> {
      // N.B.: All stats handling is async
      long preHandlingTimeNS = System.nanoTime();
      stats.ifPresent(clientStats -> clientStats.recordRequestSerializationTime(
          LatencyUtils.convertLatencyFromNSToMS(preSubmitTimeInNS - preRequestTimeInNS)));
      stats.ifPresent(clientStats -> clientStats.recordRequestSubmissionToResponseHandlingTime(
          LatencyUtils.convertLatencyFromNSToMS(preHandlingTimeNS - preSubmitTimeInNS)));

      return responseHandler.handle(clientResponse, throwable, () ->
          stats.ifPresent(clientStats ->
              clientStats.recordResponseDeserializationTime(LatencyUtils.getLatencyInMS(preHandlingTimeNS))));
    };

    if (handleResponseOnDeserializationExecutor) {
      return transportFuture.handleAsync(responseHandlerWithStats, deserializationExecutor);
    } else {
      return transportFuture.handle(responseHandlerWithStats);
    }
  }

  /**
   * Calls the overloaded function of the same, but executes the deserialization time reporting after the
   * {@param responseHandler} has returned.
   *
   * @see {@link #requestSubmissionWithStatsHandling(Optional, long, boolean, Supplier, ResponseHandler)}
   */
  private <R> CompletableFuture<R> requestSubmissionWithStatsHandling(
      final Optional<ClientStats> stats,
      final long preRequestTimeInNS,
      final boolean handleResponseOnDeserializationExecutor,
      final Supplier<CompletableFuture<TransportClientResponse>> requestSubmitter,
      final BiFunction<TransportClientResponse, Throwable, R> responseHandler) throws VeniceClientException {
    return requestSubmissionWithStatsHandling(
        stats,
        preRequestTimeInNS,
        handleResponseOnDeserializationExecutor,
        requestSubmitter,
        (clientResponse, throwable, responseDeserializationComplete) -> {
          try {
            return responseHandler.apply(clientResponse, throwable);
          } finally {
            responseDeserializationComplete.report();
          }
        }
    );
  }

  private interface ResponseHandler<R> {
    R handle(TransportClientResponse clientResponse, Throwable throwable, Reporter responseDeserializationComplete);
  }

  @Override
  public ComputeRequestBuilder<K> compute(Optional<ClientStats> stats, Optional<ClientStats> streamingStats, final long preRequestTimeInNS) {
    return compute(stats, streamingStats, this, preRequestTimeInNS);
  }

  @Override
  public ComputeRequestBuilder<K> compute(final Optional<ClientStats> stats, final Optional<ClientStats> streamingStats,
      final InternalAvroStoreClient computeStoreClient, final long preRequestTimeInNS)  {
    return new AvroComputeRequestBuilderV1<>(getLatestValueSchema(), computeStoreClient, stats, streamingStats);
  }


  private byte[] serializeComputeRequest(List<K> keyList, byte[] serializedComputeRequest) {
    List<ByteBuffer> serializedKeyList = new ArrayList<>();
    keyList.stream().forEach(key -> serializedKeyList.add(ByteBuffer.wrap(getKeySerializer().serialize(key))));
    return  computeRequestClientKeySerializer.serializeObjects(serializedKeyList, ByteBuffer.wrap(serializedComputeRequest));
  }
  /**
   * This function is only being used by {@link AvroComputeRequestBuilderV1#execute(Set)}.
   *
   * @param computeRequestWrapper
   * @param keys
   * @param resultSchema
   * @param stats
   * @param preRequestTimeInNS
   * @return
   */
  @Override
  public CompletableFuture<Map<K, GenericRecord>> compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys,
      Schema resultSchema, Optional<ClientStats> stats, final long preRequestTimeInNS) {
    if (keys.isEmpty()) {
      return COMPLETABLE_FUTURE_FOR_EMPTY_KEY_IN_COMPUTE;
    }
    List<K> keyList = new ArrayList<>(keys);
    byte[] serializedComputeRequest = computeRequestWrapper.serialize();
    byte[] serializedFullComputeRequest = serializeComputeRequest(keyList, serializedComputeRequest);
    CompletableFuture<Map<K, GenericRecord>> valueFuture = new CompletableFuture();
    final Map<String, String> headerMap = (computeRequestWrapper.getComputeRequestVersion() == COMPUTE_REQUEST_VERSION_V1)
        ? COMPUTE_HEADER_MAP_V1
        : COMPUTE_HEADER_MAP_V2;

    requestSubmissionWithStatsHandling(
        stats,
        preRequestTimeInNS,
        true,
        () -> transportClient.post(getComputeRequestPath(), headerMap, serializedFullComputeRequest),
        (clientResponse, throwable, responseDeserializationComplete) -> {
          try {
            if (null != throwable) {
              // Error response, such as 400 or 5xx will be handled here.
              valueFuture.completeExceptionally(throwable);
            } else if (null == clientResponse) {
              // Even all the keys don't exist in Venice, compute should receive empty result instead of 'null'
              valueFuture.completeExceptionally(new VeniceClientException("TransportClient should not return null for compute request"));
            } else if (!clientResponse.isSchemaIdValid()) {
              valueFuture.completeExceptionally(new VeniceClientException("No valid schema id received for compute request!"));
            } else {
              int responseSchemaId = clientResponse.getSchemaId();
              long preResponseEnvelopeDeserialization = System.nanoTime();

              RecordDeserializer<ComputeResponseRecordV1> deserializer = getComputeResponseRecordDeserializer(responseSchemaId);
              Iterable<ComputeResponseRecordV1> records = deserializer.deserializeObjects(new ByteBufferOptimizedBinaryDecoder(clientResponse.getBody()));
              RecordDeserializer<GenericRecord> dataDeserializer = getComputeResultRecordDeserializer(resultSchema);

              computeDeserializer.deserialize(valueFuture, records, keyList, (resultMap, envelope) -> {
                int keyIdx = envelope.keyIndex;
                if (keyIdx >= keyList.size() || keyIdx < 0) {
                  throw new VeniceClientException("Key index: " + keyIdx + " doesn't have a corresponding key");
                }
                GenericRecord value = dataDeserializer.deserialize(envelope.value);
                /**
                 * Wrap up the returned {@link GenericRecord} to throw exception when retrieving some failed
                 * computation.
                 */
                resultMap.put(keyList.get(keyIdx), ComputeGenericRecord.wrap(value));
              }, responseDeserializationComplete, stats, preResponseEnvelopeDeserialization);
            }
          } catch (Exception e) {
            if (! valueFuture.isDone()) {
              valueFuture.completeExceptionally(e);
            } else {
              // Defensive code
              logger.error("Received exception after completing the `valueFuture` for compute request", e);
              throw e;
            }
          }

          return null;
        });

    return valueFuture;
  }

  @Override
  public void start() throws VeniceClientException {
    if (needSchemaReader) {
      //TODO: remove the 'instanceof' statement once HttpClient got refactored.
      if (transportClient instanceof D2TransportClient) {
        this.schemaReader = new SchemaReader(this, this.getReaderSchema());
      } else {
        this.schemaReader = new SchemaReader(this.getStoreClientForSchemaReader(), this.getReaderSchema());
      }
    } else {
      this.schemaReader = null;
    }
  }

  @Override
  public void close() {
    boolean isHttp = transportClient instanceof HttpTransportClient;
    IOUtils.closeQuietly(transportClient);
    if (isHttp) { // TODO make d2client close method idempotent.  d2client re-uses the transport client for the schema reader
      IOUtils.closeQuietly(schemaReader);
    }
  }

  protected Optional<Schema> getReaderSchema() {
    return Optional.empty();
  }

  protected abstract AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader();

  public abstract RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException;

  private void validateMultiGetResponseSchemaId(int schemaId) {
    int protocolVersion = ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
    if (protocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + protocolVersion);
    }
  }

  private RecordDeserializer<MultiGetResponseRecordV1> getMultiGetResponseRecordDeserializer(int schemaId) {
    // TODO: get multi-get response write schema from Router
    validateMultiGetResponseSchemaId(schemaId);
    if (useFastAvro) {
      return FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(MultiGetResponseRecordV1.SCHEMA$,
          MultiGetResponseRecordV1.class, multiGetEnvelopeIterableImpl);
    } else {
      return SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class,
          multiGetEnvelopeIterableImpl);
    }
  }

  private void validateComputeResponseSchemaId(int schemaId) {
    int protocolVersion = ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion();
    if (protocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + protocolVersion);
    }
  }

  private RecordDeserializer<ComputeResponseRecordV1> getComputeResponseRecordDeserializer(int schemaId) {
    validateComputeResponseSchemaId(schemaId);
    if (useFastAvro) {
      return FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(ComputeResponseRecordV1.SCHEMA$,
          ComputeResponseRecordV1.class);
    } else {
      return SerializerDeserializerFactory.getAvroSpecificDeserializer(ComputeResponseRecordV1.class);
    }
  }

  private RecordDeserializer<GenericRecord> getComputeResultRecordDeserializer(Schema resultSchema) {
    if (useFastAvro) {
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(resultSchema, resultSchema);
    }
    return SerializerDeserializerFactory.getAvroGenericDeserializer(resultSchema);
  }

  public String toString() {
    return this.getClass().getSimpleName() +
        "(storeName: " + storeName +
        ", transportClient: " + transportClient.toString() + ")";
  }

  // For testing usage.
  protected void setD2ServiceDiscovery(D2ServiceDiscovery d2ServiceDiscovery){
    this.d2ServiceDiscovery = d2ServiceDiscovery;
  }


  @Override
  public Schema getKeySchema() {
    /**
     * Leveraging the following function to do safe D2 discovery for the following schema fetches.
      */
    getKeySerializer();
    return schemaReader.getKeySchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    /**
     * Leveraging the following function to do safe D2 discovery for the following schema fetches.
     */
    getKeySerializer();
    return schemaReader.getLatestValueSchema();
  }

  private static ByteBuffer decompressRecord(CompressionStrategy compressionStrategy, ByteBuffer data) {
    try {
      return CompressorFactory.getCompressor(compressionStrategy).decompress(data);
    } catch (IOException e) {
      throw new VeniceClientException(
          String.format("Unable to decompress the record, compressionStrategy=%d", compressionStrategy.getValue()), e);
    }
  }

  private interface DeserializerFunc<ENVELOPE, V> {
    public V deserialize(ENVELOPE envelope, CompressionStrategy compressionStrategy);
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
    private int duplicateEntryCnt = 0;
    private Optional<StreamingFooterRecordV1> streamingFooterRecord = Optional.empty();
    private Optional<TrackingStreamingCallback> trackingStreamingCallback = Optional.empty();

    private List<CompletableFuture<Void>> callbackFutures = new ArrayList<>();

    public StoreClientStreamingCallback(List<K> keyList,
        StreamingCallback<K, V> callback,
        Function<Integer, ReadEnvelopeChunkedDeserializer<ENVELOPE>> envelopeDeserializerFunc,
        DeserializerFunc<ENVELOPE, V> recordDeserializerFunc,
        Function<ENVELOPE, Integer> indexRetrievalFunc,
        Function<ENVELOPE, StreamingFooterRecordV1> streamingFooterRecordDeserializer) {
      this.keyList = keyList;
      this.callback = callback;
      if (callback instanceof TrackingStreamingCallback) {
        trackingStreamingCallback = Optional.of((TrackingStreamingCallback)callback);
      }
      this.envelopeDeserializerFunc = envelopeDeserializerFunc;
      this.recordDeserializerFunc = recordDeserializerFunc;
      this.indexRetrievalFunc = indexRetrievalFunc;
      this.receivedKeySet = new BitSet(keyList.size());
      this.streamingFooterRecordDeserializer = streamingFooterRecordDeserializer;
    }

    @Override
    public void onHeaderReceived(Map<String, String> headers) {
      isStreamingResponse = headers.containsKey(HttpConstants.VENICE_STREAMING_RESPONSE);
      String schemaIdHeader = headers.get(HttpConstants.VENICE_SCHEMA_ID);
      if (null != schemaIdHeader) {
        responseSchemaId = Integer.parseInt(schemaIdHeader);
      }
      envelopeDeserializer = envelopeDeserializerFunc.apply(responseSchemaId);
      String compressionHeader = headers.get(HttpConstants.VENICE_COMPRESSION_STRATEGY);
      if (null != compressionHeader) {
        compressionStrategy = CompressionStrategy.valueOf(Integer.valueOf(compressionHeader));
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
      throw new VeniceClientException("Invalid key index: " + keyIdx + ", either it should be the footer"
          + " record key index: " + KEY_ID_FOR_STREAMING_FOOTER + " or its absolute value should be [0, " + keyList.size() + ")");
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
        for (ENVELOPE record : availableRecords) {
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
            value = recordDeserializerFunc.deserialize(record, compressionStrategy);
          }
          trackingStreamingCallback.ifPresent( t -> t.onRecordDeserialized());
          resultMap.put(key, value);
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
                  completedException = Optional.of(new VeniceClientHttpException(streamingFooterRecord.get().detail.toString(), streamingFooterRecord.get().status));
                } else if (throwable != null) {
                  if (throwable instanceof Exception) {
                    completedException = Optional.of((Exception)throwable);
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
                      }
                    }
                  }
                }
            callback.onCompletion(completedException);
            final Optional<Exception> finalCompletedException = completedException;
            trackingStreamingCallback.ifPresent(t -> t.onDeserializationCompletion(finalCompletedException, receivedKeySet.cardinality(),
                duplicateEntryCnt));
              }
          );
    }
  }

  private boolean handleCallbackForEmptyKeySet(Set<K> keys, StreamingCallback callback) {
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
    List<K> keyList = new ArrayList<>(keys);
    byte[] multiGetBody = serializeMultiGetRequest(keyList);
    Map<Integer, RecordDeserializer<V>> deserializerCache = new VeniceConcurrentHashMap<>();

    transportClient.streamPost(getStorageRequestPath(), MULTI_GET_HEADER_MAP_FOR_STREAMING, multiGetBody,
        new StoreClientStreamingCallback<>(
            keyList,
            callback,
            envelopeSchemaId -> {
              validateMultiGetResponseSchemaId(envelopeSchemaId);
              return new MultiGetResponseRecordV1ChunkedDeserializer();
            },
            (envelope, compressionStrategy) -> {
              RecordDeserializer<V> recordDeserializer = deserializerCache.computeIfAbsent(envelope.schemaId, id -> getDataRecordDeserializer(id));
              ByteBuffer decompressedValue = decompressRecord(compressionStrategy, envelope.value);
              return recordDeserializer.deserialize(decompressedValue);
            },
            envelope -> envelope.keyIndex,
            envelope -> streamingFooterRecordDeserializer.deserialize(envelope.value)
        )
    );
  }

  @Override
  public void compute(ComputeRequestWrapper computeRequestWrapper, Set<K> keys, Schema resultSchema,
      StreamingCallback<K, GenericRecord> callback, long preRequestTimeInNS) throws VeniceClientException
  {
    if (handleCallbackForEmptyKeySet(keys, callback)) {
      // empty key set
      return;
    }
    List<K> keyList = new ArrayList<>(keys);
    byte[] serializedComputeRequest = computeRequestWrapper.serialize();
    byte[] serializedFullComputeRequest = serializeComputeRequest(keyList, serializedComputeRequest);
    final Map<String, String> headerMap = (computeRequestWrapper.getComputeRequestVersion() == COMPUTE_REQUEST_VERSION_V1)
        ? COMPUTE_HEADER_MAP_FOR_STREAMING_V1
        : COMPUTE_HEADER_MAP_FOR_STREAMING_V2;

    RecordDeserializer<GenericRecord> computeResultRecordDeserializer = getComputeResultRecordDeserializer(resultSchema);

    transportClient.streamPost(getComputeRequestPath(), headerMap, serializedFullComputeRequest,
        new StoreClientStreamingCallback<>(
            keyList,
            callback,
            envelopeSchemaId -> {
              validateComputeResponseSchemaId(envelopeSchemaId);
              return new ComputeResponseRecordV1ChunkedDeserializer();
            },
            // Compute doesn't support compression
            (envelope, compressionStrategy) -> computeResultRecordDeserializer.deserialize(envelope.value),
            envelope -> envelope.keyIndex,
            envelope -> streamingFooterRecordDeserializer.deserialize(envelope.value)
        )
    );
  }
}
