package com.linkedin.venice.fastclient;

import static com.linkedin.venice.HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID;
import static com.linkedin.venice.client.store.AbstractAvroStoreClient.TYPE_COMPUTE;

import com.linkedin.restli.common.HttpStatus;
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
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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
  }

  protected StoreMetadata getStoreMetadata() {
    return metadata;
  }

  private String composeURIForSingleGet(GetRequestContext requestContext, K key) {
    int currentVersion = getCurrentVersion();
    String resourceName = getResourceName(currentVersion);
    long nanoTsBeforeSerialization = System.nanoTime();
    byte[] keyBytes = keySerializer.serialize(key);
    requestContext.requestSerializationTime = getLatencyInNS(nanoTsBeforeSerialization);
    int partitionId = metadata.getPartitionId(currentVersion, keyBytes);
    String b64EncodedKeyBytes = EncodingUtils.base64EncodeToString(keyBytes);

    requestContext.currentVersion = currentVersion;
    requestContext.partitionId = partitionId;

    String sb = URI_SEPARATOR + AbstractAvroStoreClient.TYPE_STORAGE + URI_SEPARATOR + resourceName + URI_SEPARATOR
        + partitionId + URI_SEPARATOR + b64EncodedKeyBytes + AbstractAvroStoreClient.B64_FORMAT;
    return sb;
  }

  private String composeURIForBatchGetRequest(MultiKeyRequestContext<K, V> requestContext) {
    int currentVersion = getCurrentVersion();
    String resourceName = getResourceName(currentVersion);

    requestContext.currentVersion = currentVersion;
    StringBuilder sb = new StringBuilder();
    sb.append(URI_SEPARATOR).append(AbstractAvroStoreClient.TYPE_STORAGE).append(URI_SEPARATOR).append(resourceName);
    return sb.toString();
  }

  private String composeURIForComputeRequest(MultiKeyRequestContext<K, V> requestContext) {
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
      CompletableFuture<HttpStatus> routeRequestFuture = null;
      try {
        String url = route + uri;
        CompletableFuture<TransportClientResponse> transportFuture = transportClient.get(url);
        routeRequestFuture =
            metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, partitionId, transportFuture);
        requestContext.routeRequestMap.put(route, routeRequestFuture);

        transportFutures.add(transportFuture);
        CompletableFuture<HttpStatus> finalRouteRequestFuture = routeRequestFuture;
        transportFuture.whenCompleteAsync((response, throwable) -> {
          if (throwable != null) {
            HttpStatus statusCode = (throwable instanceof VeniceClientHttpException)
                ? HttpStatus.fromCode(((VeniceClientHttpException) throwable).getHttpStatus())
                : HttpStatus.S_503_SERVICE_UNAVAILABLE;
            finalRouteRequestFuture.complete(statusCode);
          } else if (response == null) {
            finalRouteRequestFuture.complete(HttpStatus.S_404_NOT_FOUND);
            if (!receivedSuccessfulResponse.getAndSet(true)) {
              requestContext.requestSubmissionToResponseHandlingTime =
                  LatencyUtils.getLatencyInMS(nanoTsBeforeSendingRequest);

              valueFuture.complete(null);
            }
          } else {
            try {
              finalRouteRequestFuture.complete(HttpStatus.S_200_OK);
              if (!receivedSuccessfulResponse.getAndSet(true)) {
                requestContext.requestSubmissionToResponseHandlingTime =
                    LatencyUtils.getLatencyInMS(nanoTsBeforeSendingRequest);
                CompressionStrategy compressionStrategy = response.getCompressionStrategy();
                long nanoTsBeforeDecompression = System.nanoTime();
                ByteBuffer data = decompressRecord(
                    compressionStrategy,
                    ByteBuffer.wrap(response.getBody()),
                    requestContext.currentVersion,
                    metadata.getCompressor(compressionStrategy, requestContext.currentVersion));
                requestContext.decompressionTime = LatencyUtils.getLatencyInMS(nanoTsBeforeDecompression);
                long nanoTsBeforeDeserialization = System.nanoTime();
                RecordDeserializer<V> deserializer = getDataRecordDeserializer(response.getSchemaId());
                V value = tryToDeserialize(deserializer, data, response.getSchemaId(), key);
                requestContext.responseDeserializationTime = LatencyUtils.getLatencyInMS(nanoTsBeforeDeserialization);
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
          routeRequestFuture = metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, partitionId, null);
          requestContext.routeRequestMap.put(route, routeRequestFuture);
        }
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
              LatencyUtils.getLatencyInMS(nanoTsBeforeSendingRequest);
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
    verifyMetadataInitialized();
    streamingBatchGetInternal(requestContext, keys, callback);

    /* Wiring in a callback for when all events have been received. If any route failed with an exception,
     * that exception will be passed to the aggregate future's next stages. */
    CompletableFuture.allOf(requestContext.getAllRouteFutures().toArray(new CompletableFuture[0]))
        .whenComplete((response, throwable) -> {
          if (throwable != null || (!keys.isEmpty() && requestContext.getAllRouteFutures().isEmpty())) {
            // If there is an exception or if no partition has a healthy replica.
            // The exception to send to the client might be different. Get from the requestContext
            Throwable clientException = throwable;
            if (requestContext.getPartialResponseException().isPresent()) {
              clientException = requestContext.getPartialResponseException().get();
            }
            callback.onCompletion(
                Optional.of(new VeniceClientException("At least one route did not complete", clientException)));
          } else if (!requestContext.isPartialSuccessAllowed
              && requestContext.getPartialResponseException().isPresent()) {
            // TODO: It would be great to move this to InternalAvroStoreClient, but the callback is a
            // "StatTrackingCallback" and it needs the exceptions to be called out to not misrepresent in the stats.
            callback.onCompletion(
                Optional.of(
                    new VeniceClientException(
                        "Response was not complete",
                        requestContext.getPartialResponseException().get())));
          } else {
            callback.onCompletion(Optional.empty());
          }
        });
  }

  /**
   * This internal method takes a batchGet request context, a set of keys and determines the strategy for scattering
   * the requests. The callback is invoked whenever a response is received from the internal transport.
   * @param requestContext
   * @param keys
   * @param callback
   */
  private void streamingBatchGetInternal(
      MultiKeyRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) {
    int keyCnt = keys.size();
    if (keyCnt > this.config.getMaxAllowedKeyCntInBatchGetReq()) {
      throw new VeniceKeyCountLimitException(
          getStoreName(),
          RequestType.MULTI_GET,
          keyCnt,
          this.config.getMaxAllowedKeyCntInBatchGetReq());
    }

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
              partId,
              1,
              requestContext.getRoutesForPartitionMapping().getOrDefault(partId, Collections.emptySet())));

      if (routes.isEmpty()) {
        /* If a partition doesn't have an available route then there is something wrong about or metadata and this is
         * an error */
        requestContext.noAvailableReplica = true;
        String errorMessage = String.format(
            "No available route for store: %s, version: %s, partitionId: %s",
            getStoreName(),
            currentVersion,
            partitionId);
        LOGGER.error(errorMessage);
        requestContext.setPartialResponseException(new VeniceClientException(errorMessage));
      }

      /* Add this key into each route we are going to send request to.
        Current implementation has only one replica/route count , so each key will go via one route.
        For loop is not necessary here but if in the future we send to multiple routes then the code below remains */
      for (String route: routes) {
        requestContext.addKey(route, key, keyBytes, partitionId);
      }
    }
    Map<String, String> headers = Collections.singletonMap(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion()));
    // Start the request and invoke handler for response
    for (String route: requestContext.getRoutes()) {
      String url = route + uriForBatchGetRequest;
      long nanoTsBeforeSerialization = System.nanoTime();
      byte[] serializedRequest = serializeMultiGetRequest(requestContext.keysForRoutes(route));
      requestContext.recordRequestSerializationTime(route, getLatencyInNS(nanoTsBeforeSerialization));
      requestContext.recordRequestSentTimeStamp(route);
      CompletableFuture<TransportClientResponse> routeFuture = transportClient.post(url, headers, serializedRequest);
      CompletableFuture<HttpStatus> routeRequestFuture =
          metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, 0, routeFuture);
      requestContext.routeRequestMap.put(route, routeRequestFuture);

      routeFuture.whenComplete((transportClientResponse, throwable) -> {
        requestContext.recordRequestSubmissionToResponseHandlingTime(route);
        TransportClientResponseForRoute response = TransportClientResponseForRoute
            .fromTransportClientWithRoute(transportClientResponse, route, routeRequestFuture);
        batchGetTransportRequestCompletionHandler(requestContext, response, throwable, callback);
      });
    }
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
      LOGGER.error("Exception received from transport. ExMsg: {}", exception.getMessage());
      requestContext.markCompleteExceptionally(transportClientResponse, exception);
      HttpStatus statusCode = (exception instanceof VeniceClientHttpException)
          ? HttpStatus.fromCode(((VeniceClientHttpException) exception).getHttpStatus())
          : HttpStatus.S_503_SERVICE_UNAVAILABLE;
      transportClientResponse.getRouteRequestFuture().complete(statusCode);
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
    RecordDeserializer<V> dataRecordDeserializer = getDataRecordDeserializer(transportClientResponse.getSchemaId());

    List<MultiKeyRequestContext.KeyInfo<K>> keyInfos =
        requestContext.keysForRoutes(transportClientResponse.getRouteId());
    Set<Integer> keysSeen = new HashSet<>();

    LOGGER.debug("Response received for route {} -> {} ", transportClientResponse.getRouteId(), records);
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
      totalDecompressionTimeForResponse += System.nanoTime() - nanoTsBeforeDecompression;

      long nanoTsBeforeDeserialization = System.nanoTime();
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
    transportClientResponse.getRouteRequestFuture().complete(HttpStatus.S_200_OK);
  }

  @Override
  public void compute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequest,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    verifyMetadataInitialized();

    computeInternal(requestContext, computeRequest, keys, resultSchema, callback)
        .whenComplete((response, throwable) -> {
          // Wiring in a callback for when all events have been received. If any route failed with an exception,
          // that exception will be passed to the aggregate future's next stages.
          if (throwable != null || (!keys.isEmpty() && requestContext.getAllRouteFutures().isEmpty())) {
            // If there is an exception or if no partition has a healthy replica.
            // The exception to send to the client might be different. Get from the requestContext
            Throwable clientException = throwable;
            if (requestContext.getPartialResponseException().isPresent()) {
              clientException = requestContext.getPartialResponseException().get();
            }
            callback.onCompletion(
                Optional.of(new VeniceClientException("At least one route did not complete", clientException)));
          } else if (!requestContext.isPartialSuccessAllowed
              && requestContext.getPartialResponseException().isPresent()) {
            // TODO: It would be great to move this to InternalAvroStoreClient, but the callback is a
            // "StatTrackingCallback" and it needs the exceptions to be called out to not misrepresent in the stats.
            callback.onCompletion(
                Optional.of(
                    new VeniceClientException(
                        "Response was not complete",
                        requestContext.getPartialResponseException().get())));
          } else {
            callback.onCompletion(Optional.empty());
          }
        });
  }

  private CompletableFuture<Void> computeInternal(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequest,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback) throws VeniceClientException {
    int keyCnt = keys.size();
    if (keyCnt > this.config.getMaxAllowedKeyCntInBatchGetReq()) {
      throw new VeniceKeyCountLimitException(
          getStoreName(),
          RequestType.COMPUTE,
          keyCnt,
          this.config.getMaxAllowedKeyCntInBatchGetReq());
    }

    requestContext.instanceHealthMonitor = metadata.getInstanceHealthMonitor();
    String uriForComputeRequest = composeURIForComputeRequest(requestContext);
    int schemaId = metadata.getValueSchemaId(computeRequest.getValueSchema());
    int currentVersion = requestContext.currentVersion;
    Map<Integer, List<String>> partitionRouteMap = new HashMap<>();
    Map<String, CompletableFuture<Void>> routeToResponseFutures = new VeniceConcurrentHashMap<>();
    for (K key: keys) {
      byte[] keyBytes = keySerializer.serialize(key);
      // For each key determine partition
      int partitionId = metadata.getPartitionId(currentVersion, keyBytes);
      // Find routes for each partition
      List<String> routes = partitionRouteMap.computeIfAbsent(
          partitionId,
          (partId) -> metadata.getReplicas(1, currentVersion, partId, 1, Collections.emptySet()));

      if (routes.isEmpty()) {
        /* If a partition doesn't have an available route then there is something wrong about or metadata and this is
         * an error */
        requestContext.noAvailableReplica = true;
        String errorMessage = String.format(
            "No available route for store: %s, version: %s, partitionId: %s",
            getStoreName(),
            currentVersion,
            partitionId);
        LOGGER.error(errorMessage);
        requestContext.setPartialResponseException(new VeniceClientException(errorMessage));
      }

      /* Add this key into each route we are going to send request to.
        Current implementation has only one replica/route count , so each key will go via one route.
        For loop is not necessary here but if in the future we send to multiple routes then the code below remains */
      for (String route: routes) {
        requestContext.addKey(route, key, keyBytes, partitionId);
        routeToResponseFutures.put(route, new CompletableFuture<>());
      }
    }

    Map<String, String> headers = new HashMap<>(2);
    headers.put(
        HttpConstants.VENICE_API_VERSION,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_REQUEST_V3.getProtocolVersion()));
    headers.put(VENICE_COMPUTE_VALUE_SCHEMA_ID, Integer.toString(schemaId));
    for (String route: requestContext.getRoutes()) {
      String url = route + uriForComputeRequest;
      long nanoTsBeforeSerialization = System.nanoTime();
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes = requestContext.keysForRoutes(route);
      byte[] serializedRequest = serializeComputeRequest(computeRequest, keysForRoutes);

      ComputeRecordStreamDecoder decoder = getComputeDecoderForRoute(
          computeRequest,
          keysForRoutes,
          resultSchema,
          callback,
          routeToResponseFutures.get(route));

      requestContext.recordRequestSerializationTime(route, getLatencyInNS(nanoTsBeforeSerialization));
      requestContext.recordRequestSentTimeStamp(route);
      CompletableFuture<TransportClientResponse> routeFuture = transportClient.post(url, headers, serializedRequest);
      CompletableFuture<HttpStatus> routeRequestFuture =
          metadata.trackHealthBasedOnRequestToInstance(route, currentVersion, 0, routeFuture);
      requestContext.routeRequestMap.put(route, routeRequestFuture);

      routeFuture.whenComplete((transportClientResponse, throwable) -> {
        requestContext.recordRequestSubmissionToResponseHandlingTime(route);
        TransportClientResponseForRoute response = TransportClientResponseForRoute
            .fromTransportClientWithRoute(transportClientResponse, route, routeRequestFuture);
        computeTransportRequestCompletionHandler(requestContext, response, throwable, decoder);
      });
    }

    return CompletableFuture.allOf(routeToResponseFutures.values().toArray(new CompletableFuture[0]));
  }

  private ComputeRecordStreamDecoder getComputeDecoderForRoute(
      ComputeRequestWrapper computeRequest,
      List<MultiKeyRequestContext.KeyInfo<K>> keysForRoutes,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> allRecordsCallback,
      CompletableFuture<Void> routeFuture) {
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
              routeFuture.completeExceptionally(exception.get());
            } else {
              routeFuture.complete(null);
            }
          }
        };

    return new ComputeRecordStreamDecoder<>(
        keyList,
        nonCompletingStreamingCallback,
        deserializationExecutor,
        STREAMING_FOOTER_RECORD_DESERIALIZER,
        getComputeResultRecordDeserializer(resultSchema));
  }

  /**
   * This callback handles results from one route for multiple keys in that route once the post()
   * is completed with {@link TransportClientResponseForRoute} for this route.
   */
  private void computeTransportRequestCompletionHandler(
      ComputeRequestContext<K, V> requestContext,
      TransportClientResponseForRoute transportClientResponse,
      Throwable exception,
      ComputeRecordStreamDecoder decoder) {
    if (exception != null) {
      LOGGER.error("Exception received from transport. ExMsg: {}", exception.getMessage());
      requestContext.markCompleteExceptionally(transportClientResponse, exception);
      HttpStatus statusCode = (exception instanceof VeniceClientHttpException)
          ? HttpStatus.fromCode(((VeniceClientHttpException) exception).getHttpStatus())
          : HttpStatus.S_503_SERVICE_UNAVAILABLE;
      decoder.onCompletion(Optional.of(new VeniceClientException("Exception received from transport", exception)));
      transportClientResponse.getRouteRequestFuture().complete(statusCode);
      return;
    }

    try {
      Map<String, String> headers = new HashMap<>(2);
      headers.put(HttpConstants.VENICE_SCHEMA_ID, String.valueOf(transportClientResponse.getSchemaId()));

      decoder.onHeaderReceived(headers);
      decoder.onDataReceived(ByteBuffer.wrap(transportClientResponse.getBody()));
      decoder.onCompletion(Optional.empty());
    } catch (Throwable t) {
      LOGGER.error("Exception while decoding compute response. ExMsg: {}", t.getMessage());
      requestContext.markCompleteExceptionally(transportClientResponse, t);
      decoder.onCompletion(Optional.of(new VeniceClientException("Failed to decode compute response", t)));
      transportClientResponse.getRouteRequestFuture().complete(HttpStatus.S_500_INTERNAL_SERVER_ERROR);
      return;
    }

    requestContext.markComplete(transportClientResponse);
    transportClientResponse.getRouteRequestFuture().complete(HttpStatus.S_200_OK);
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

  private RecordDeserializer<GenericRecord> getComputeResultRecordDeserializer(Schema resultSchema) {
    return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(resultSchema, resultSchema);
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

  @Override
  public Schema getLatestValueSchema() {
    return metadata.getLatestValueSchema();
  }
}
