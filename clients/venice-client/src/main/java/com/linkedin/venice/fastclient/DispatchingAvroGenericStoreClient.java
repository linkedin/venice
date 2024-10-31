package com.linkedin.venice.fastclient;

import static com.linkedin.venice.client.store.AbstractAvroStoreClient.TYPE_COMPUTE;
import static org.apache.hc.core5.http.HttpStatus.SC_BAD_GATEWAY;
import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.streaming.ComputeRecordStreamDecoder;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.TrackingStreamingCallback;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.GrpcTransportClient;
import com.linkedin.venice.fastclient.transport.R2TransportClient;
import com.linkedin.venice.fastclient.transport.TransportClientResponseForRoute;
import com.linkedin.venice.read.RequestHeadersProvider;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.AvroStoreDeserializerCache;
import com.linkedin.venice.serialization.StoreDeserializerCache;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.ByteBufferOptimizedBinaryDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is in charge of routing and serialization/de-serialization.
 */
public class DispatchingAvroGenericStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong();

  private static final Logger LOGGER = LogManager.getLogger(DispatchingAvroGenericStoreClient.class);
  private static final String URI_SEPARATOR = "/";
  private static final Executor DESERIALIZATION_EXECUTOR = AbstractAvroStoreClient.getDefaultDeserializationExecutor();
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  private final String BATCH_GET_TRANSPORT_EXCEPTION_FILTER_MESSAGE;
  private final String COMPUTE_TRANSPORT_EXCEPTION_FILTER_MESSAGE;

  protected final StoreMetadata metadata;
  private final int requiredReplicaCount;

  private final ClientConfig config;
  private final TransportClient transportClient;
  private final Executor deserializationExecutor;

  // Key serializer
  private RecordSerializer<K> keySerializer;
  protected StoreDeserializerCache<V> storeDeserializerCache;

  private static final RecordSerializer<MultiGetRouterRequestKeyV1> MULTI_GET_REQUEST_SERIALIZER =
      FastSerializerDeserializerFactory.getAvroGenericSerializer(MultiGetRouterRequestKeyV1.SCHEMA$);
  private static final RecordSerializer<ComputeRouterRequestKeyV1> COMPUTE_REQUEST_SERIALIZER =
      FastSerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.SCHEMA$);
  private static final RecordDeserializer<StreamingFooterRecordV1> STREAMING_FOOTER_RECORD_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(StreamingFooterRecordV1.SCHEMA$, StreamingFooterRecordV1.class);

  public DispatchingAvroGenericStoreClient(StoreMetadata metadata, ClientConfig config) {
    /**
     * If the client is configured to use gRPC, we create a {@link GrpcTransportClient} where we also pass
     * a standard {@link R2TransportClient} to handle the non-storage related requests as we haven't yet
     * implemented these actions in gRPC, yet.
     */
    this(
        metadata,
        config,
        config.useGrpc()
            ? new GrpcTransportClient(config.getGrpcClientConfig())
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
    String storeName = metadata.getStoreName();
    BATCH_GET_TRANSPORT_EXCEPTION_FILTER_MESSAGE = "BatchGet Transport Exception for " + storeName;
    COMPUTE_TRANSPORT_EXCEPTION_FILTER_MESSAGE = "Compute Transport Exception for " + storeName;
    this.storeDeserializerCache = new AvroStoreDeserializerCache<>(metadata);
  }

  protected StoreMetadata getStoreMetadata() {
    return metadata;
  }

  private String composeURIForSingleGet(GetRequestContext requestContext, K key) {
    int currentVersion = getCurrentVersion();
    String resourceName = getResourceName(currentVersion);
    long nanoTsBeforeSerialization = System.nanoTime();
    byte[] keyBytes = keySerializer.serialize(key);
    requestContext.requestSerializationTime = LatencyUtils.getElapsedTimeFromNSToMS(nanoTsBeforeSerialization);
    int partitionId = metadata.getPartitionId(currentVersion, keyBytes);
    String b64EncodedKeyBytes = EncodingUtils.base64EncodeToString(keyBytes);

    requestContext.currentVersion = currentVersion;
    requestContext.partitionId = partitionId;

    String sb = URI_SEPARATOR + AbstractAvroStoreClient.TYPE_STORAGE + URI_SEPARATOR + resourceName + URI_SEPARATOR
        + partitionId + URI_SEPARATOR + b64EncodedKeyBytes + AbstractAvroStoreClient.B64_FORMAT;
    return sb;
  }

  private String composeRouteForBatchGetRequest(BatchGetRequestContext<K, V> requestContext) {
    int currentVersion = getCurrentVersion();
    String resourceName = getResourceName(currentVersion);

    requestContext.currentVersion = currentVersion;
    StringBuilder sb = new StringBuilder();
    sb.append(URI_SEPARATOR).append(AbstractAvroStoreClient.TYPE_STORAGE).append(URI_SEPARATOR).append(resourceName);
    return sb.toString();
  }

  private String composeRouteForComputeRequest(ComputeRequestContext<K, V> requestContext) {
    int currentVersion = getCurrentVersion();
    String resourceName = getResourceName(currentVersion);

    requestContext.currentVersion = currentVersion;
    StringBuilder sb = new StringBuilder();
    sb.append(URI_SEPARATOR).append(TYPE_COMPUTE).append(URI_SEPARATOR).append(resourceName);
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
  public ClientConfig getClientConfig() {
    return config;
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext requestContext, K key) throws VeniceClientException {
    verifyMetadataInitialized();
    requestContext.serverClusterName = metadata.getClusterName();
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
    long nanoTsBeforeSendingRequest = System.nanoTime();

    /**
     * Check {@link StoreMetadata#getReplicas} to understand why the below method
     * might return more than required number of routes
     */
    List<String> routes = metadata.getReplicas(
        REQUEST_ID_GENERATOR.getAndIncrement(),
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
      CompletableFuture<Integer> routeRequestFuture = null;
      try {
        String url = route + uri;
        CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(url);
        routeRequestFuture =
            metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, partitionId, transportFuture)
                .getOriginalFuture();
        requestContext.routeRequestMap.put(route, routeRequestFuture);

        transportFutures.add(transportFuture);
        CompletableFuture<Integer> finalRouteRequestFuture = routeRequestFuture;
        transportFuture.whenCompleteAsync((response, throwable) -> {
          if (throwable != null) {
            finalRouteRequestFuture.completeExceptionally(throwable);
          } else if (response == null) {
            finalRouteRequestFuture.complete(SC_NOT_FOUND);
            if (!receivedSuccessfulResponse.getAndSet(true)) {
              requestContext.requestSubmissionToResponseHandlingTime =
                  LatencyUtils.getElapsedTimeFromNSToMS(nanoTsBeforeSendingRequest);

              valueFuture.complete(null);
            }
          } else {
            try {
              finalRouteRequestFuture.complete(SC_OK);
              if (!receivedSuccessfulResponse.getAndSet(true)) {
                requestContext.requestSubmissionToResponseHandlingTime =
                    LatencyUtils.getElapsedTimeFromNSToMS(nanoTsBeforeSendingRequest);
                CompressionStrategy compressionStrategy = response.getCompressionStrategy();
                long nanoTsBeforeDecompression = System.nanoTime();
                ByteBuffer data = decompressRecord(
                    compressionStrategy,
                    ByteBuffer.wrap(response.getBody()),
                    requestContext.currentVersion,
                    metadata.getCompressor(compressionStrategy, requestContext.currentVersion));
                requestContext.decompressionTime = LatencyUtils.getElapsedTimeFromNSToMS(nanoTsBeforeDecompression);
                long nanoTsBeforeDeserialization = System.nanoTime();
                RecordDeserializer<V> deserializer = getDataRecordDeserializer(response.getSchemaId());
                V value = tryToDeserialize(deserializer, data, response.getSchemaId(), key);
                requestContext.responseDeserializationTime =
                    LatencyUtils.getElapsedTimeFromNSToMS(nanoTsBeforeDeserialization);
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
        if (routeRequestFuture == null) {
          // to update health data, create a future if the exception was thrown before it could be created
          routeRequestFuture = metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, partitionId, null)
              .getOriginalFuture();
          requestContext.routeRequestMap.put(route, routeRequestFuture);
        }
        routeRequestFuture.completeExceptionally(e);
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
      CompletableFuture.allOf(transportFutures.toArray(new CompletableFuture[0])).exceptionally(throwable -> {
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
              LatencyUtils.getElapsedTimeFromNSToMS(nanoTsBeforeSendingRequest);
          valueFuture.completeExceptionally(throwable);
        }
        return null;
      });
    }
    return valueFuture;
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
    multiKeyStreamingRequest(
        requestContext,
        RequestType.MULTI_GET_STREAMING,
        keys,
        callback,
        composeRouteForBatchGetRequest(requestContext),
        RequestHeadersProvider.getStreamingBatchGetHeaders(keys.size()),
        this::serializeMultiGetRequest,
        (MultiKeyStreamingRouteResponseHandler<K>) (
            keysForRoutes,
            response,
            throwable) -> batchGetTransportRequestCompletionHandler(requestContext, response, throwable, callback));
  }

  private interface MultiKeyStreamingRouteResponseHandler<K> {
    /**
     * Multi-key requests might be routed to different server hosts and this class offers a way to handle the response
     * per route, and it is responsible for:
     * 1. Marking the original {@link RequestContext} as completed (successfully or exceptionally).
     * 2. Completing the {@link TransportClientResponseForRoute#getRouteRequestFuture()} with appropriate HTTP status
     * codes for that route after the response has been processed completely. (200 and 404 are considered SUCCESS).
     */
    void handle(
        List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
        TransportClientResponseForRoute transportClientResponse,
        Throwable exception);
  }

  /**
   * This internal method offers a generic way to perform scatter-gather operations on a set of keys.
   * The function determines the strategy for scattering the requests, triggers the requests, and waits for all requests
   * to be completed and responses to be processed.
   * @param requestContext The context that is used to track the state of the request.
   * @param requestType The type of the request.
   * @param keys The set of keys to be queried.
   * @param callback When all routes have completed, the {@link StreamingCallback#onCompletion(Optional)} is triggered.
   * @param routeForMultiKeyRequest The endpoint on the servers that the POST request will be sent to
   * @param requestHeaders The headers to be sent with the request
   * @param requestSerializer The function that serializes the request from a list of keys to a byte array. This will form the body of the request.
   * @param routeResponseHandler The callback is invoked whenever a response is received from the internal transport.
   *                             It is responsible for invoking {@link StreamingCallback#onRecordReceived(Object, Object)}
   *                             on the {@param callback} function for each key, marking the {@param requestContext} as
   *                             completed, and for completing the {@link TransportClientResponseForRoute#getRouteRequestFuture()} for that route.
   */
  private void multiKeyStreamingRequest(
      MultiKeyRequestContext<K, V> requestContext,
      RequestType requestType,
      Set<K> keys,
      StreamingCallback callback,
      String routeForMultiKeyRequest,
      Map<String, String> requestHeaders,
      Function<List<MultiKeyRequestContext.KeyInfo<K>>, byte[]> requestSerializer,
      MultiKeyStreamingRouteResponseHandler routeResponseHandler) {
    verifyMetadataInitialized();
    int keyCnt = keys.size();
    if (keyCnt > metadata.getBatchGetLimit()) {
      VeniceKeyCountLimitException veniceKeyCountLimitException =
          new VeniceKeyCountLimitException(getStoreName(), requestType, keyCnt, metadata.getBatchGetLimit());
      callback.onCompletion(Optional.of(veniceKeyCountLimitException));
      return;
    }

    long requestId = REQUEST_ID_GENERATOR.getAndIncrement();

    requestContext.serverClusterName = metadata.getClusterName();
    /* Prepare each of the routes needed to query the keys */
    requestContext.instanceHealthMonitor = metadata.getInstanceHealthMonitor();
    int currentVersion = requestContext.currentVersion;
    Map<Integer, List<String>> partitionRouteMap = new HashMap<>();
    Set<String> partitionsWithNoRoutes = new ConcurrentSkipListSet<>();
    for (K key: keys) {
      byte[] keyBytes = keySerializer.serialize(key);
      // For each key determine partition
      int partitionId = metadata.getPartitionId(currentVersion, keyBytes);
      // Find routes for each partition
      List<String> routes = partitionRouteMap.computeIfAbsent(
          partitionId,
          (ignored) -> metadata.getReplicas(
              requestId,
              currentVersion,
              partitionId,
              1,
              requestContext.getRoutesForPartitionMapping().getOrDefault(partitionId, Collections.emptySet())));

      if (routes.isEmpty()) {
        /* If a partition doesn't have an available route then there is something wrong about or metadata and this is
         * an error */
        requestContext.noAvailableReplica = true;
        partitionsWithNoRoutes.add(String.valueOf(partitionId));
      }

      /* Add this key into each route we are going to send request to.
        Current implementation has only one replica/route count , so each key will go via one route.
        For loop is not necessary here but if in the future we send to multiple routes then the code below remains */
      for (String route: routes) {
        requestContext.addKey(route, key, keyBytes, partitionId);
      }
    }

    requestContext.setFanoutSize(requestContext.getRoutes().size());
    int numberOfRequestCompletionFutures =
        requestContext.getRoutes().size() + (partitionsWithNoRoutes.isEmpty() ? 0 : 1);
    CompletableFuture<Integer>[] requestCompletionFutures = new CompletableFuture[numberOfRequestCompletionFutures];

    int routeIndex = 0;
    // Start the request and invoke handler for response
    for (String route: requestContext.getRoutes()) {
      String url = route + routeForMultiKeyRequest;
      long nanoTsBeforeSerialization = System.nanoTime();
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes = requestContext.keysForRoutes(route);
      byte[] serializedRequest = requestSerializer.apply(keysForRoutes);
      requestContext.recordRequestSerializationTime(route, getLatencyInNS(nanoTsBeforeSerialization));
      requestContext.recordRequestSentTimeStamp(route);
      CompletableFuture<TransportClientResponse> transportClientFutureForRoute =
          transportClient.post(url, requestHeaders, serializedRequest);
      ChainedCompletableFuture<Integer, Integer> routeRequestFuture =
          metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, 0, transportClientFutureForRoute);
      requestContext.routeRequestMap.put(route, routeRequestFuture.getOriginalFuture());
      requestCompletionFutures[routeIndex] = routeRequestFuture.getResultFuture();

      transportClientFutureForRoute.whenComplete((transportClientResponse, throwable) -> {
        requestContext.recordRequestSubmissionToResponseHandlingTime(route);
        TransportClientResponseForRoute response = TransportClientResponseForRoute
            .fromTransportClientWithRoute(transportClientResponse, route, routeRequestFuture.getOriginalFuture());
        routeResponseHandler.handle(keysForRoutes, response, throwable);
      });
      routeIndex++;
    }

    if (!partitionsWithNoRoutes.isEmpty()) {
      String errorMessage = String.format(
          "No available route for store: %s, version: %s, partitionIds: %s",
          getStoreName(),
          currentVersion,
          String.join(", ", partitionsWithNoRoutes));
      // TODO: Explore if we need to use a different message as the filter for the redundant filter as different
      // partition sets will have different error messages
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(errorMessage)) {
        LOGGER.error(errorMessage);
      }
      VeniceClientHttpException clientException = new VeniceClientHttpException(errorMessage, SC_BAD_GATEWAY);
      requestContext.setPartialResponseExceptionIfNull(clientException);
      CompletableFuture<Integer> placeholderFailedFuture = new CompletableFuture<>();
      placeholderFailedFuture.completeExceptionally(clientException);
      requestCompletionFutures[routeIndex] = placeholderFailedFuture;
    }

    CompletableFuture.allOf(requestCompletionFutures).whenComplete((response, throwable) -> {
      requestContext.complete();

      boolean failedOverall = throwable != null;
      if (!failedOverall) {
        for (CompletableFuture<Integer> requestCompletionFuture: requestCompletionFutures) {
          try {
            int status = requestCompletionFuture.get();
            if (status != SC_OK && status != SC_NOT_FOUND) {
              failedOverall = true;
              break;
            }
          } catch (Exception e) {
            failedOverall = true;
            break;
          }
        }
      }

      // Wiring in a callback for when all events have been received. If any route failed with an exception,
      // that exception will be passed to the aggregate future's next stages.
      if (failedOverall && !requestContext.isPartialSuccessAllowed) {
        // The exception to send to the client might be different. Get from the requestContext
        Throwable clientException = throwable;
        if (requestContext.getPartialResponseException().isPresent()) {
          clientException = requestContext.getPartialResponseException().get();
        }
        callback.onCompletion(
            Optional.of(new VeniceClientException("At least one route did not complete", clientException)));
      } else {
        callback.onCompletion(Optional.empty());
      }
    });
  }

  /**
   * This callback handles results from one route for multiple keys in that route once the post()
   * is completed with {@link TransportClientResponseForRoute} for this route.
   */
  private void batchGetTransportRequestCompletionHandler(
      MultiKeyRequestContext<K, V> requestContext,
      TransportClientResponseForRoute transportClientResponse,
      Throwable exception,
      StreamingCallback<K, V> callback) {
    if (exception != null) {
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(BATCH_GET_TRANSPORT_EXCEPTION_FILTER_MESSAGE)) {
        LOGGER.error("Exception received from transport. ExMsg: {}", exception.getMessage());
      }

      requestContext.markCompleteExceptionally(transportClientResponse, exception);
      transportClientResponse.getRouteRequestFuture().completeExceptionally(exception);
      return;
    }
    // deserialize records and find the status
    RecordDeserializer<MultiGetResponseRecordV1> deserializer =
        getMultiGetResponseRecordDeserializer(transportClientResponse.getSchemaId());
    long nanoTsBeforeRequestDeserialization = System.nanoTime();
    Iterable<MultiGetResponseRecordV1> records =
        deserializer.deserializeObjects(new ByteBufferOptimizedBinaryDecoder(transportClientResponse.getBody()));
    requestContext.recordRequestDeserializationTime(
        transportClientResponse.getRouteId(),
        getLatencyInNS(nanoTsBeforeRequestDeserialization));

    List<MultiKeyRequestContext.KeyInfo<K>> keyInfos =
        requestContext.keysForRoutes(transportClientResponse.getRouteId());
    Set<Integer> keysSeen = new HashSet<>();

    long totalDecompressionTimeForResponse = 0;
    VeniceCompressor compressor =
        metadata.getCompressor(transportClientResponse.getCompressionStrategy(), requestContext.currentVersion);
    for (MultiGetResponseRecordV1 r: records) {
      long nanoTsBeforeDecompression = System.nanoTime();

      ByteBuffer decompressRecord = decompressRecord(
          transportClientResponse.getCompressionStrategy(),
          r.value,
          requestContext.currentVersion,
          compressor);

      long nanoTsBeforeDeserialization = System.nanoTime();
      totalDecompressionTimeForResponse += nanoTsBeforeDeserialization - nanoTsBeforeDecompression;
      RecordDeserializer<V> dataRecordDeserializer = getDataRecordDeserializer(r.getSchemaId());
      V deserializedValue = dataRecordDeserializer.deserialize(decompressRecord);
      requestContext.recordRecordDeserializationTime(
          transportClientResponse.getRouteId(),
          getLatencyInNS(nanoTsBeforeDeserialization));
      MultiKeyRequestContext.KeyInfo<K> k = keyInfos.get(r.keyIndex);
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
    transportClientResponse.getRouteRequestFuture().complete(SC_OK);
  }

  @Override
  protected void compute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequest,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    verifyMetadataInitialized();

    RecordDeserializer<GenericRecord> computeResultRecordDeserializer =
        getComputeResultRecordDeserializer(resultSchema);
    // TODO: client side compute is not supported for fast-client yet, hence hard coding isRemoteComputationOnly to true
    multiKeyStreamingRequest(
        requestContext,
        RequestType.COMPUTE_STREAMING,
        keys,
        callback,
        composeRouteForComputeRequest(requestContext),
        RequestHeadersProvider.getStreamingComputeHeaderMap(keys.size(), computeRequest.getValueSchemaID(), true),
        (keysForRoutes) -> serializeComputeRequest(computeRequest, keysForRoutes),
        (MultiKeyStreamingRouteResponseHandler<K>) (keysForRoutes, response, throwable) -> {
          ComputeRecordStreamDecoder decoder = getComputeDecoderForRoute(
              requestContext,
              computeRequest,
              keysForRoutes,
              response,
              computeResultRecordDeserializer,
              callback);
          computeTransportRequestCompletionHandler(response, throwable, decoder);
        });
  }

  private ComputeRecordStreamDecoder getComputeDecoderForRoute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequest,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      TransportClientResponseForRoute transportClientResponse,
      RecordDeserializer<GenericRecord> computeResultRecordDeserializer,
      StreamingCallback<K, ComputeGenericRecord> allRecordsCallback) {
    List<K> keyList = new ArrayList<>(keysForRoutes.size());
    for (MultiKeyRequestContext.KeyInfo keyInfo: keysForRoutes) {
      keyList.add((K) keyInfo.getKey());
    }

    // Don't want it to mark the future for all routes complete
    TrackingStreamingCallback<K, GenericRecord> nonCompletingStreamingCallback =
        new TrackingStreamingCallback<K, GenericRecord>() {
          @Override
          public Optional<ClientStats> getStats() {
            return Optional.empty();
          }

          @Override
          public void onRecordDeserialized() {
          }

          @Override
          public void onDeserializationCompletion(
              Optional<Exception> exception,
              int successKeyCount,
              int duplicateEntryCount) {
          }

          @Override
          public void onRecordReceived(K key, GenericRecord value) {
            allRecordsCallback.onRecordReceived(
                key,
                value != null ? new ComputeGenericRecord(value, computeRequest.getValueSchema()) : null);
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            // Don't complete the main callback here. It will be completed when all routes are done.
            if (exception.isPresent()) {
              Exception e = exception.get();
              requestContext.markCompleteExceptionally(transportClientResponse, e);
              transportClientResponse.getRouteRequestFuture().completeExceptionally(e);
            } else {
              requestContext.markComplete(transportClientResponse);
              transportClientResponse.getRouteRequestFuture().complete(SC_OK);
            }
          }
        };

    return new ComputeRecordStreamDecoder<>(
        keyList,
        nonCompletingStreamingCallback,
        deserializationExecutor,
        STREAMING_FOOTER_RECORD_DESERIALIZER,
        computeResultRecordDeserializer);
  }

  /**
   * This callback handles results from one route for multiple keys in that route once the post()
   * is completed with {@link TransportClientResponseForRoute} for this route.
   */
  private void computeTransportRequestCompletionHandler(
      TransportClientResponseForRoute transportClientResponse,
      Throwable exception,
      ComputeRecordStreamDecoder decoder) {
    if (exception != null) {
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(COMPUTE_TRANSPORT_EXCEPTION_FILTER_MESSAGE)) {
        LOGGER.error("Exception received from transport. ExMsg: {}", exception.getMessage());
      }
      VeniceClientException clientException;
      if (exception instanceof VeniceClientException) {
        clientException = (VeniceClientException) exception;
      } else {
        clientException = new VeniceClientException("Exception received from transport", exception);
      }
      decoder.onCompletion(Optional.of(clientException));
      return;
    }

    try {
      Map<String, String> headers = Collections
          .singletonMap(HttpConstants.VENICE_SCHEMA_ID, String.valueOf(transportClientResponse.getSchemaId()));

      decoder.onHeaderReceived(headers);
      decoder.onDataReceived(ByteBuffer.wrap(transportClientResponse.getBody()));
      decoder.onCompletion(Optional.empty());
    } catch (Throwable t) {
      LOGGER.error("Exception while decoding compute response. ExMsg: {}", t.getMessage());
      decoder.onCompletion(
          Optional.of(new VeniceClientHttpException("Failed to decode compute response", SC_INTERNAL_SERVER_ERROR, t)));
    }
  }

  private byte[] serializeComputeRequest(
      ComputeRequestWrapper computeRequest,
      List<MultiKeyRequestContext.KeyInfo<K>> keyList) {
    List<ComputeRouterRequestKeyV1> routerRequestKeys = new ArrayList<>(keyList.size());
    for (int i = 0; i < keyList.size(); i++) {
      MultiKeyRequestContext.KeyInfo<K> keyInfo = keyList.get(i);
      ComputeRouterRequestKeyV1 routerRequestKey = new ComputeRouterRequestKeyV1();
      byte[] keyBytes = keyInfo.getSerializedKey();
      routerRequestKey.keyBytes = ByteBuffer.wrap(keyBytes);
      routerRequestKey.keyIndex = i;
      routerRequestKey.partitionId = keyInfo.getPartitionId();
      routerRequestKeys.add(routerRequestKey);
    }

    return COMPUTE_REQUEST_SERIALIZER.serializeObjects(routerRequestKeys, ByteBuffer.wrap(computeRequest.serialize()));
  }

  /* Batch get helper methods */
  protected RecordDeserializer<MultiGetResponseRecordV1> getMultiGetResponseRecordDeserializer(int schemaId) {
    // TODO: get multi-get response write schema from Router

    return FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(MultiGetResponseRecordV1.SCHEMA$, MultiGetResponseRecordV1.class);
  }

  protected RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
    return storeDeserializerCache.getDeserializer(schemaId, metadata.getLatestValueSchemaId());
  }

  private RecordDeserializer<GenericRecord> getComputeResultRecordDeserializer(Schema resultSchema) {
    return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(resultSchema, resultSchema);
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

  private byte[] serializeMultiGetRequest(List<MultiKeyRequestContext.KeyInfo<K>> keyList) {
    List<MultiGetRouterRequestKeyV1> routerRequestKeys = new ArrayList<>(keyList.size());
    for (int i = 0; i < keyList.size(); i++) {
      MultiKeyRequestContext.KeyInfo<K> keyInfo = keyList.get(i);
      MultiGetRouterRequestKeyV1 routerRequestKey = new MultiGetRouterRequestKeyV1();
      byte[] keyBytes = keyInfo.getSerializedKey();
      ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
      routerRequestKey.keyBytes = keyByteBuffer;
      routerRequestKey.keyIndex = i;
      routerRequestKey.partitionId = keyInfo.getPartitionId();
      routerRequestKeys.add(routerRequestKey);
    }
    return MULTI_GET_REQUEST_SERIALIZER.serializeObjects(routerRequestKeys);
  }

  private long getLatencyInNS(long startTimeStamp) {
    return System.nanoTime() - startTimeStamp;
  }

  void verifyMetadataInitialized() throws VeniceClientException {
    if (!metadata.isReady()) {
      throw new VeniceClientException(metadata.getStoreName() + " metadata is not ready yet, retry in sometime");
    }
    // initialize keySerializer here as it depends on the metadata's key schema
    if (keySerializer == null) {
      keySerializer = getKeySerializer(getKeySchema());
    }
  }

  @Override
  public void start() throws VeniceClientException {
    metadata.start();
  }

  protected RecordSerializer getKeySerializer(Schema keySchema) {
    return FastSerializerDeserializerFactory.getFastAvroGenericSerializer(keySchema);
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

  @Deprecated
  @Override
  public Schema getLatestValueSchema() {
    return metadata.getLatestValueSchema();
  }

  @Override
  public SchemaReader getSchemaReader() {
    return metadata;
  }
}
