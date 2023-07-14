package com.linkedin.venice.fastclient;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.GrpcTransportClient;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.fastclient.transport.TransportClientResponseForRoute;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.avro.Schema;
import org.apache.avro.io.ByteBufferOptimizedBinaryDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is in charge of routing and serialization/de-serialization.
 */
public class DispatchingAvroGenericStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(DispatchingAvroGenericStoreClient.class);
  private static final String URI_SEPARATOR = "/";
  private static final Executor DESERIALIZATION_EXECUTOR = AbstractAvroStoreClient.getDefaultDeserializationExecutor();

  private final StoreMetadata metadata;
  private final int requiredReplicaCount;

  private final ClientConfig config;
  private final TransportClient transportClient;
  private final Executor deserializationExecutor;

  // Key serializer
  private RecordSerializer<K> keySerializer;
  private RecordSerializer<MultiGetRouterRequestKeyV1> multiGetSerializer;

  public DispatchingAvroGenericStoreClient(StoreMetadata metadata, ClientConfig config) {
    /**
     * If the client is configured to use gRPC, we create a {@link GrpcTransportClient} where we also pass
     * a standard {@link R2TransportClient} to handle the metadata related requests as we haven't yet
     * implemented the metadata related requests in gRPC.
     */
    this(
        metadata,
        config,
        config.useGrpc()
            ? new GrpcTransportClient(
                config.getNettyServerToGrpcAddressMap(),
                new R2TransportClient(config.getR2Client()))
            : new R2TransportClient(config.getR2Client()));
  }

  // Visible for testing
  public DispatchingAvroGenericStoreClient(
      StoreMetadata metadata,
      ClientConfig config,
      TransportClient transportClient) {
    this.metadata = metadata;
    this.config = config;
    this.transportClient = transportClient;

    if (config.isSpeculativeQueryEnabled()) {
      this.requiredReplicaCount = 2;
    } else {
      this.requiredReplicaCount = 1;
    }

    this.deserializationExecutor =
        Optional.ofNullable(config.getDeserializationExecutor()).orElse(DESERIALIZATION_EXECUTOR);
  }

  protected StoreMetadata getStoreMetadata() {
    return metadata;
  }

  private String composeURIForSingleGet(GetRequestContext requestContext, K key) {
    int currentVersion = getCurrentVersion();
    String resourceName = getResourceName(currentVersion);
    long beforeSerializationTimeStamp = System.nanoTime();
    byte[] keyBytes = keySerializer.serialize(key);
    requestContext.requestSerializationTime = getLatencyInNS(beforeSerializationTimeStamp);
    int partitionId = metadata.getPartitionId(currentVersion, keyBytes);
    String b64EncodedKeyBytes = EncodingUtils.base64EncodeToString(keyBytes);

    requestContext.currentVersion = currentVersion;
    requestContext.partitionId = partitionId;

    String sb = URI_SEPARATOR + AbstractAvroStoreClient.TYPE_STORAGE + URI_SEPARATOR + resourceName + URI_SEPARATOR
        + partitionId + URI_SEPARATOR + b64EncodedKeyBytes + AbstractAvroStoreClient.B64_FORMAT;
    return sb;
  }

  private String composeURIForBatchGetRequest(BatchGetRequestContext<K, V> requestContext) {
    int currentVersion = getCurrentVersion();
    String resourceName = getResourceName(currentVersion);

    requestContext.currentVersion = currentVersion;
    StringBuilder sb = new StringBuilder();
    sb.append(URI_SEPARATOR).append(AbstractAvroStoreClient.TYPE_STORAGE).append(URI_SEPARATOR).append(resourceName);
    return sb.toString();
  }

  private String getResourceName(int currentVersion) {
    return metadata.getStoreName() + "_v" + currentVersion;
  }

  private int getCurrentVersion() {
    int currentVersion = metadata.getCurrentStoreVersion();
    if (currentVersion <= 0) {
      throw new VeniceClientException("No available current version, please do a push first");
    }
    return currentVersion;
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    verifyMetadataInitialized();
    requestContext.instanceHealthMonitor = metadata.getInstanceHealthMonitor();
    if (requestContext.requestUri == null) {
      /**
       * Reuse the request uri for the retry request.
       */
      requestContext.requestUri = composeURIForSingleGet(requestContext, key);
    }
    final String uri = requestContext.requestUri;

    int currentVersion = requestContext.currentVersion;
    int partitionId = requestContext.partitionId;

    CompletableFuture<V> valueFuture = new CompletableFuture<>();
    long timestampBeforeSendingRequest = System.nanoTime();

    /**
     * Check {@link StoreMetadata#getReplicas} to understand why the below method
     * might return more than required number of routes
     */
    List<String> routes = metadata.getReplicas(
        requestContext.requestId,
        currentVersion,
        partitionId,
        requiredReplicaCount,
        requestContext.routeRequestMap.keySet());
    if (routes.isEmpty()) {
      requestContext.noAvailableReplica = true;
      valueFuture.completeExceptionally(
          new VeniceClientException(
              "No available route for store: " + getStoreName() + ", version: " + currentVersion + ", partition: "
                  + partitionId));
      return valueFuture;
    }

    /**
     * This atomic variable is used to find the fastest response received and ignore other responses
     */
    AtomicBoolean receivedSuccessfulResponse = new AtomicBoolean(false);

    /**
     * List of futures used below and their relationships:
     * 1. valueFuture => this is the completable future that is returned from this function which either throws an exception or
     *                    returns null or returns value.
     * 2. routeRequestFuture => an incomplete completable future returned from {@link InstanceHealthMonitor} for
     *                          {@link AbstractStoreMetadata} by adjusting {@link InstanceHealthMonitor#pendingRequestCounterMap}
     *                          for each server instances per store before starting a get request and during completing this future.
     *                          This is also added to {@link requestContext.routeRequestMap} to indicate whether a particular server
     *                          instance is already used for this get request: to not reuse the same instance for both the queries
     *                          for a key when speculative query is enabled.
     * 3. transportFuture => completable future for the actual get() operation to the server. This future was passed as callback to
     *                       the async call {@link Client#restRequest} and once get() is done, this will be completed. When this is
     *                       completed, it will also
     *                       1. complete routeRequestFuture by passing in the status (200/404/etc) which will handle(decrement)
     *                          {@link InstanceHealthMonitor#pendingRequestCounterMap}
     *                       2. complete valueFuture by passing in either null/value/exception
     * 4. transportFutures => List of transportFuture for this request (eg: 2 if speculative query is enabled or even more depends on
     *                        health of the replicas).
     */
    List<CompletableFuture<TransportClientResponse>> transportFutures = new LinkedList<>();
    requestContext.requestSentTimestampNS = System.nanoTime();
    for (String route: routes) {
      CompletableFuture<HttpStatus> routeRequestFuture =
          metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, partitionId);
      requestContext.routeRequestMap.put(route, routeRequestFuture);
      try {
        String url = route + uri;
        CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(url);
        transportFutures.add(transportFuture);
        transportFuture.whenCompleteAsync((response, throwable) -> {
          if (throwable != null) {
            HttpStatus statusCode = (throwable instanceof VeniceClientHttpException)
                ? HttpStatus.fromCode(((VeniceClientHttpException) throwable).getHttpStatus())
                : HttpStatus.S_503_SERVICE_UNAVAILABLE;
            routeRequestFuture.complete(statusCode);
          } else if (response == null) {
            routeRequestFuture.complete(HttpStatus.S_404_NOT_FOUND);
            if (!receivedSuccessfulResponse.getAndSet(true)) {
              requestContext.requestSubmissionToResponseHandlingTime =
                  LatencyUtils.getLatencyInMS(timestampBeforeSendingRequest);

              valueFuture.complete(null);
            }
          } else {
            try {
              routeRequestFuture.complete(HttpStatus.S_200_OK);
              if (!receivedSuccessfulResponse.getAndSet(true)) {
                requestContext.requestSubmissionToResponseHandlingTime =
                    LatencyUtils.getLatencyInMS(timestampBeforeSendingRequest);
                CompressionStrategy compressionStrategy = response.getCompressionStrategy();
                long timestampBeforeDecompression = System.nanoTime();
                ByteBuffer data = decompressRecord(
                    compressionStrategy,
                    ByteBuffer.wrap(response.getBody()),
                    requestContext.currentVersion,
                    metadata.getCompressor(compressionStrategy, requestContext.currentVersion));
                requestContext.decompressionTime = LatencyUtils.getLatencyInMS(timestampBeforeDecompression);
                long timestampBeforeDeserialization = System.nanoTime();
                RecordDeserializer<V> deserializer = getDataRecordDeserializer(response.getSchemaId());
                V value = tryToDeserialize(deserializer, data, response.getSchemaId(), key);
                requestContext.responseDeserializationTime =
                    LatencyUtils.getLatencyInMS(timestampBeforeDeserialization);
                requestContext.successRequestKeyCount.incrementAndGet();
                valueFuture.complete(value);
              }
            } catch (Exception e) {
              if (!valueFuture.isDone()) {
                valueFuture.completeExceptionally(e);
              }
            }
          }
        }, deserializationExecutor);
      } catch (Exception e) {
        LOGGER.error("Received exception while sending request to route: {}", route, e);
        routeRequestFuture.complete(HttpStatus.S_503_SERVICE_UNAVAILABLE);
      }
    }
    if (transportFutures.isEmpty()) {
      // No request has been sent out: Routes were found, but get() failed. But setting a generic exception for now.
      valueFuture.completeExceptionally(
          new VeniceClientException(
              "No available replica for store: " + getStoreName() + ", version: " + currentVersion + " and partition: "
                  + partitionId));
      // TODO: metrics?
    } else {
      CompletableFuture.allOf(transportFutures.toArray(new CompletableFuture[transportFutures.size()]))
          .exceptionally(throwable -> {
            boolean allFailed = true;
            for (CompletableFuture transportFuture: transportFutures) {
              if (!transportFuture.isCompletedExceptionally()) {
                allFailed = false;
                break;
              }
            }
            if (allFailed) {
              // Only fail the request if all the transport futures are completed exceptionally.
              requestContext.requestSubmissionToResponseHandlingTime =
                  LatencyUtils.getLatencyInMS(timestampBeforeSendingRequest);
              valueFuture.completeExceptionally(throwable);
            }
            return null;
          });
    }
    return valueFuture;
  }

  /**
   * batchGet using streamingBatchGet implementation
   */
  @Override
  protected CompletableFuture<Map<K, V>> batchGet(BatchGetRequestContext<K, V> requestContext, Set<K> keys)
      throws VeniceClientException {
    verifyMetadataInitialized();
    CompletableFuture<Map<K, V>> responseFuture = new CompletableFuture<>();
    CompletableFuture<VeniceResponseMap<K, V>> streamingResponseFuture = streamingBatchGet(requestContext, keys);
    streamingResponseFuture.whenComplete((response, throwable) -> {
      if (throwable != null) {
        responseFuture.completeExceptionally(throwable);
      } else if (!response.isFullResponse()) {
        if (requestContext.getPartialResponseException().isPresent()) {
          responseFuture.completeExceptionally(
              new VeniceClientException(
                  "Response was not complete",
                  requestContext.getPartialResponseException().get()));
        } else {
          responseFuture.completeExceptionally(new VeniceClientException("Response was not complete"));
        }
      } else {
        responseFuture.complete(response);
      }
    });
    return responseFuture;
  }

  @Override
  protected CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys) throws VeniceClientException {
    verifyMetadataInitialized();
    // keys that do not exist in the storage nodes
    Queue<K> nonExistingKeys = new ConcurrentLinkedQueue<>();
    VeniceConcurrentHashMap<K, V> valueMap = new VeniceConcurrentHashMap<>();
    CompletableFuture<VeniceResponseMap<K, V>> streamingResponseFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl<K, V>(valueMap, nonExistingKeys, false),
        keys.size(),
        Optional.empty());
    streamingBatchGet(requestContext, keys, new StreamingCallback<K, V>() {
      @Override
      public void onRecordReceived(K key, V value) {
        if (value == null) {
          nonExistingKeys.add(key);
        } else {
          requestContext.successRequestKeyCount.incrementAndGet();
          valueMap.put(key, value);
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        requestContext.complete();
        if (exception.isPresent()) {
          streamingResponseFuture.completeExceptionally(exception.get());
        } else {
          streamingResponseFuture.complete(new VeniceResponseMapImpl<>(valueMap, nonExistingKeys, true));
        }
      }
    });
    return streamingResponseFuture;
  }

  /**
   *  This is the main implementation of the "streaming" version of batch get. As such this API doesn't provide a way
   *  to handle early exceptions. Further we tend to mix callback style and future style of asynchronous programming
   *  which makes it hard to make flexible and composable abstractions on top of this. For future enhancements we
   *  could consider returning a java stream , a bounded stream in the shape of a
   *  flux (project reactor), or one of the similar java 9 flow constructs.
   * @param requestContext
   * @param keys
   * @param callback
   */
  @Override
  protected void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) {
    verifyMetadataInitialized();
    /* This implementation is intentionally designed to separate the request phase (scatter) and the response handling
     * phase (gather). These internal methods help to keep this separation and leaves room for future fine-grained control. */
    streamingBatchGetInternal(requestContext, keys, (transportClientResponse, throwable) -> {
      // This method binds the internal transport client response to the events delivered to the callback
      transportRequestCompletionHandler(requestContext, transportClientResponse, throwable, callback);
    });

    /* Wiring in a callback for when all events have been received. If any route failed with an exception,
     * that exception will be passed to the aggregate future's next stages. */
    CompletableFuture.allOf(requestContext.getAllRouteFutures().toArray(new CompletableFuture[0]))
        .whenComplete((response, throwable) -> {
          if (throwable == null) {
            callback.onCompletion(Optional.empty());
          } else {
            // The exception to send to the client might be different. Get from the requestContext
            Throwable clientException = throwable;
            if (requestContext.getPartialResponseException().isPresent()) {
              clientException = requestContext.getPartialResponseException().get();
            }
            callback.onCompletion(
                Optional.of(new VeniceClientException("At least one route did not complete", clientException)));
          }
        });
  }

  /**
   * This internal method takes a batchGet request context, a set of keys and determines the strategy for scattering
   * the requests. The callback is invoked whenever a response is received from the internal transport.
   * @param requestContext
   * @param keys
   * @param transportClientResponseCompletionHandler
   */
  private void streamingBatchGetInternal(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      BiConsumer<TransportClientResponseForRoute, Throwable> transportClientResponseCompletionHandler) {
    /* Prepare each of the routes needed to query the keys */
    requestContext.instanceHealthMonitor = metadata.getInstanceHealthMonitor();
    String uriForBatchGetRequest = composeURIForBatchGetRequest(requestContext);
    int currentVersion = requestContext.currentVersion;
    Map<Integer, List<String>> partitionRouteMap = new HashMap<>();
    for (K key: keys) {
      byte[] keyBytes = keySerializer.serialize(key);
      // For each key determine partition
      int partitionId = metadata.getPartitionId(currentVersion, keyBytes);
      // Find routes for each partition
      List<String> routes = partitionRouteMap.computeIfAbsent(
          partitionId,
          (partId) -> metadata.getReplicas(
              requestContext.requestId,
              currentVersion,
              partitionId,
              1,
              requestContext.getRoutesForPartitionMapping().getOrDefault(partId, Collections.emptySet())));

      if (routes.isEmpty()) {
        /* If a partition doesn't have an available route then there is something wrong about or metadata and this is
         * an error */
        requestContext.noAvailableReplica = true;
        String errorMessage = String.format(
            "No route found for partitionId: %s, store: %s, version: %s",
            partitionId,
            getStoreName(),
            currentVersion);
        LOGGER.error(errorMessage);
        requestContext.setPartialResponseException(new VeniceClientException(errorMessage));
      }

      /* Add this key into each route  we are going to send request to.
        Current implementation has only one replica/route count , so each key will go via one route.
        For loop is not necessary here but if in the future we send to multiple routes then the code below remains */
      for (String route: routes) {
        requestContext.addKey(route, key, partitionId);
      }
    }
    // Start the request and invoke handler for response
    for (String route: requestContext.getRoutes()) {
      String url = route + uriForBatchGetRequest;
      Map<String, String> headers = new HashMap<>();
      headers.put(
          HttpConstants.VENICE_API_VERSION,
          Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion()));
      long tsBeforeSerialization = System.nanoTime();
      byte[] serializedKeys = serializeMultiGetRequest(requestContext.keysForRoutes(route));
      requestContext.recordRequestSerializationTime(route, getLatencyInNS(tsBeforeSerialization));
      requestContext.recordRequestSentTimeStamp(route);
      transportClient.post(url, headers, serializedKeys).whenComplete((transportClientResponse, throwable) -> {
        requestContext.recordRequestSubmissionToResponseHandlingTime(route);
        TransportClientResponseForRoute response =
            TransportClientResponseForRoute.fromTransportClientWithRoute(transportClientResponse, route);
        transportClientResponseCompletionHandler.accept(response, throwable);
      });
    }
  }

  /**
   * This callback handles results from one route for multiple keys in that route once the post()
   * is completed with {@link TransportClientResponseForRoute} for this route.
   */
  private void transportRequestCompletionHandler(
      BatchGetRequestContext<K, V> requestContext,
      TransportClientResponseForRoute transportClientResponse,
      Throwable exception,
      StreamingCallback<K, V> callback) {
    if (exception != null) {
      LOGGER.error("Exception received from transport. ExMsg: {}", exception.getMessage());
      requestContext.markCompleteExceptionally(transportClientResponse, exception);
      return;
    }
    // deserialize records and find the status
    RecordDeserializer<MultiGetResponseRecordV1> deserializer =
        getMultiGetResponseRecordDeserializer(transportClientResponse.getSchemaId());
    long timestampBeforeRequestDeserialization = System.nanoTime();
    Iterable<MultiGetResponseRecordV1> records =
        deserializer.deserializeObjects(new ByteBufferOptimizedBinaryDecoder(transportClientResponse.getBody()));
    requestContext.recordRequestDeserializationTime(
        transportClientResponse.getRouteId(),
        getLatencyInNS(timestampBeforeRequestDeserialization));
    RecordDeserializer<V> dataRecordDeserializer = getDataRecordDeserializer(transportClientResponse.getSchemaId());

    List<BatchGetRequestContext.KeyInfo<K>> keyInfos =
        requestContext.keysForRoutes(transportClientResponse.getRouteId());
    Set<Integer> keysSeen = new HashSet<>();

    LOGGER.debug("Response received for route {} -> {} ", transportClientResponse.getRouteId(), records);
    long totalDecompressionTimeForResponse = 0;
    VeniceCompressor compressor =
        metadata.getCompressor(transportClientResponse.getCompressionStrategy(), requestContext.currentVersion);
    for (MultiGetResponseRecordV1 r: records) {
      long timeStampBeforeDecompression = System.nanoTime();
      ByteBuffer decompressRecord = decompressRecord(
          transportClientResponse.getCompressionStrategy(),
          r.value,
          requestContext.currentVersion,
          compressor);
      totalDecompressionTimeForResponse += System.nanoTime() - timeStampBeforeDecompression;

      long timeStampBeforeDeserialization = System.nanoTime();
      V deserializedValue = dataRecordDeserializer.deserialize(decompressRecord);
      requestContext.recordRecordDeserializationTime(
          transportClientResponse.getRouteId(),
          getLatencyInNS(timeStampBeforeDeserialization));
      BatchGetRequestContext.KeyInfo<K> k = keyInfos.get(r.keyIndex);
      keysSeen.add(r.keyIndex);
      callback.onRecordReceived(k.getKey(), deserializedValue);
    }
    requestContext.recordDecompressionTime(transportClientResponse.getRouteId(), totalDecompressionTimeForResponse);
    for (int i = 0; i < keyInfos.size(); i++) {
      if (!keysSeen.contains(i)) {
        callback.onRecordReceived(keyInfos.get(i).getKey(), null);
      }
    }
    requestContext.markComplete(transportClientResponse);
  }

  /* Batch get helper methods */
  protected RecordDeserializer<MultiGetResponseRecordV1> getMultiGetResponseRecordDeserializer(int schemaId) {
    // TODO: get multi-get response write schema from Router

    return FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(MultiGetResponseRecordV1.SCHEMA$, MultiGetResponseRecordV1.class);
  }

  protected RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
    Schema readerSchema = metadata.getLatestValueSchema();
    if (readerSchema == null) {
      throw new VeniceClientException("Failed to get latest value schema for store: " + metadata.getStoreName());
    }
    Schema writerSchema = metadata.getValueSchema(schemaId);
    if (writerSchema == null) {
      throw new VeniceClientException(
          "Failed to get writer schema with id: " + schemaId + " from store: " + metadata.getStoreName());
    }
    return getValueDeserializer(writerSchema, readerSchema);
  }

  protected RecordDeserializer<V> getValueDeserializer(Schema writerSchema, Schema readerSchema) {
    return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema);
  }

  private <T> T tryToDeserialize(RecordDeserializer<T> dataDeserializer, ByteBuffer data, int writerSchemaId, K key) {
    return AbstractAvroStoreClient.tryToDeserializeWithVerboseLogging(
        dataDeserializer,
        data,
        writerSchemaId,
        key,
        keySerializer,
        metadata,
        LOGGER);
  }

  private ByteBuffer decompressRecord(
      CompressionStrategy compressionStrategy,
      ByteBuffer data,
      int version,
      VeniceCompressor compressor) {
    try {
      if (compressor == null) {
        throw new VeniceClientException(
            String.format(
                "Expected to find compressor in metadata but found null, compressionStrategy:%s, store:%s, version:%d",
                compressionStrategy,
                getStoreName(),
                version));
      }
      return compressor.decompress(data);
    } catch (Exception e) {
      throw new VeniceClientException(
          String.format(
              "Unable to decompress the record, compressionStrategy:%s store:%s version:%d",
              compressionStrategy,
              getStoreName(),
              version),
          e);
    }
  }

  /* Short utility methods */

  private byte[] serializeMultiGetRequest(List<BatchGetRequestContext.KeyInfo<K>> keyList) {
    List<MultiGetRouterRequestKeyV1> routerRequestKeys = new ArrayList<>(keyList.size());
    BatchGetRequestContext.KeyInfo<K> keyInfo;
    for (int i = 0; i < keyList.size(); i++) {
      keyInfo = keyList.get(i);
      MultiGetRouterRequestKeyV1 routerRequestKey = new MultiGetRouterRequestKeyV1();
      byte[] keyBytes = keySerializer.serialize(keyInfo.getKey());
      ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
      routerRequestKey.keyBytes = keyByteBuffer;
      routerRequestKey.keyIndex = i;
      routerRequestKey.partitionId = keyInfo.getPartitionId();
      routerRequestKeys.add(routerRequestKey);
    }
    return multiGetSerializer.serializeObjects(routerRequestKeys);
  }

  private long getLatencyInNS(long startTimeStamp) {
    return System.nanoTime() - startTimeStamp;
  }

  public void verifyMetadataInitialized() throws VeniceClientException {
    if (!metadata.isReady()) {
      throw new VeniceClientException(metadata.getStoreName() + " metadata is not ready, attempting to re-initialize");
    }
    // initialize keySerializer here as it depends on the metadata's key schema
    if (keySerializer == null) {
      keySerializer = getKeySerializer(getKeySchema());
    }
  }

  @Override
  public void start() throws VeniceClientException {
    metadata.start();

    this.multiGetSerializer =
        FastSerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);
  }

  protected RecordSerializer getKeySerializer(Schema keySchema) {
    return FastSerializerDeserializerFactory.getAvroGenericSerializer(keySchema);
  }

  @Override
  public void close() {
    try {
      metadata.close();
    } catch (Exception e) {
      throw new VeniceClientException("Failed to close store metadata", e);
    }
  }

  @Override
  public String getStoreName() {
    return metadata.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    return metadata.getKeySchema();
  }

  @Override
  public Schema getLatestValueSchema() {
    return metadata.getLatestValueSchema();
  }

  // Visible for testing
  public RecordSerializer<K> getKeySerializer() {
    return keySerializer;
  }

  // Visible for testing
  public RecordSerializer<MultiGetRouterRequestKeyV1> getMultiGetSerializer() {
    return multiGetSerializer;
  }
}
