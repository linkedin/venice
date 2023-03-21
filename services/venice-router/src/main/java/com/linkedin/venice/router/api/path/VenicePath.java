package com.linkedin.venice.router.api.path;

import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VeniceResponseDecompressor;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMethod;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpUriRequest;


public abstract class VenicePath implements ResourcePath<RouterKey> {
  private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong(0);

  private final String resourceName;
  private Collection<RouterKey> partitionKeys;
  private final String storeName;
  private int versionNumber;
  private final Time time;
  private boolean retryRequest = false;
  private final boolean smartLongTailRetryEnabled;
  private final int smartLongTailRetryAbortThresholdMs;
  private long originalRequestStartTs = -1;
  private int longTailRetryThresholdMs = Integer.MAX_VALUE;
  /**
   * This slow storage node set, which will be decided by the scattered requests of the original request.
   * And this set is mostly used to decide whether we should send retry request to any specific storage node or not.
   *
   * The detailed steps:
   * 1. Add all the storage nodes which the original scattered request will be sent to the set.
   * 2. Remove the storage node from the set if the corresponding request is finished.
   * When retry happens, the set will contain the storage nodes, which haven't finished the original scattered requests,
   * which will be treated as slow storage nodes, and Router will try to avoid retry requests to the storage nodes from this set.
   */
  private Set<String> slowStorageNodeSet = new ConcurrentSkipListSet<>();
  // Whether the request supports streaming or not
  private Optional<VeniceChunkedResponse> chunkedResponse = Optional.empty();
  // Response decompressor
  private Optional<VeniceResponseDecompressor> responseDecompressor = Optional.empty();
  private long requestId = -1;
  private int helixGroupId = -1;

  public VenicePath(String resourceName, boolean smartLongTailRetryEnabled, int smartLongTailRetryAbortThresholdMs) {
    this(resourceName, smartLongTailRetryEnabled, smartLongTailRetryAbortThresholdMs, new SystemTime());
  }

  public VenicePath(
      String resourceName,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      Time time) {
    this.resourceName = resourceName;
    this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    this.versionNumber = Version.parseVersionFromKafkaTopicName(resourceName);
    this.smartLongTailRetryEnabled = smartLongTailRetryEnabled;
    this.smartLongTailRetryAbortThresholdMs = smartLongTailRetryAbortThresholdMs;
    this.time = time;
  }

  public synchronized long getRequestId() {
    if (requestId < 0) {
      requestId = REQUEST_ID_GENERATOR.getAndIncrement();
    }
    return requestId;
  }

  public int getHelixGroupId() {
    return helixGroupId;
  }

  public void setHelixGroupId(int helixGroupId) {
    this.helixGroupId = helixGroupId;
  }

  public boolean isSmartLongTailRetryEnabled() {
    return smartLongTailRetryEnabled;
  }

  public int getSmartLongTailRetryAbortThresholdMs() {
    return smartLongTailRetryAbortThresholdMs;
  }

  protected void setPartitionKeys(Collection<RouterKey> keys) {
    this.partitionKeys = keys;
  }

  @Nonnull
  @Override
  public Collection<RouterKey> getPartitionKeys() {
    return this.partitionKeys;
  }

  public int getRequestSize() {
    // The final single-element array is being used in closure since closure can only operate final variables.
    final int[] size = { 0 };
    getPartitionKeys().stream().forEach(key -> size[0] += key.getKeyBuffer().remaining());

    return size[0];
  }

  public int getVersionNumber() {
    return this.versionNumber;
  }

  @Nonnull
  @Override
  public String getResourceName() {
    return this.resourceName;
  }

  public String getStoreName() {
    return this.storeName;
  }

  @Override
  public void setRetryRequest() {
    this.retryRequest = true;
  }

  protected void setupRetryRelatedInfo(VenicePath originalPath) {
    if (originalPath.isRetryRequest()) {
      setRetryRequest();
    }

    setLongTailRetryThresholdMs(originalPath.getLongTailRetryThresholdMs());
    /**
     * All the sub-requests and retry requests for a multi-get request will share the same slow
     * storage node set.
     */
    slowStorageNodeSet = originalPath.slowStorageNodeSet;
    setOriginalRequestStartTs(originalPath.getOriginalRequestStartTs());

    this.chunkedResponse = originalPath.chunkedResponse;
    this.responseDecompressor = originalPath.responseDecompressor;

    this.requestId = originalPath.getRequestId();
    this.helixGroupId = originalPath.getHelixGroupId();
  }

  public boolean isRetryRequest() {
    return this.retryRequest;
  }

  public long getOriginalRequestStartTs() {
    return originalRequestStartTs;
  }

  private void setOriginalRequestStartTs(long originalRequestStartTs) {
    this.originalRequestStartTs = originalRequestStartTs;
  }

  public int getLongTailRetryThresholdMs() {
    return longTailRetryThresholdMs;
  }

  public void setLongTailRetryThresholdMs(int longTailRetryThresholdMs) {
    this.longTailRetryThresholdMs = longTailRetryThresholdMs;
  }

  public void requestStorageNode(String storageNode) {
    if (!isRetryRequest()) {
      /**
       * Only make decision based on the original request.
       */
      slowStorageNodeSet.add(storageNode);
    }
  }

  public void markStorageNodeAsFast(String fastStorageNode) {
    if (!isRetryRequest()) {
      /**
       * Only make decision based on the original request.
       */
      slowStorageNodeSet.remove(fastStorageNode);
    }
  }

  /**
   * This function is used to check whether Router could send retry request to the specified storage node.
   * It will return false if the requested storage node has been marked as slow.
   *
   * @param storageNode
   * @return
   */
  public boolean canRequestStorageNode(String storageNode) {
    if (!smartLongTailRetryEnabled) {
      return true;
    }
    return !isRetryRequest() || // original request
        !slowStorageNodeSet.contains(storageNode); // retry request
  }

  public void recordOriginalRequestStartTimestamp() {
    if (!isRetryRequest()) {
      setOriginalRequestStartTs(time.getMilliseconds());
    }
  }

  /**
   * This function will check whether the retry request already passed the retry delay threshold.
   * If yes, return true.
   * @return
   */
  public boolean isRetryRequestTooLate() {
    if (!smartLongTailRetryEnabled) {
      return false;
    }
    if (isRetryRequest()) {
      // Retry request
      long retryDelay = time.getMilliseconds() - getOriginalRequestStartTs();
      long smartRetryThreshold = getLongTailRetryThresholdMs() + getSmartLongTailRetryAbortThresholdMs();
      if (retryDelay > smartRetryThreshold) {
        return true;
      }
    }
    return false;
  }

  public void setupVeniceHeaders(BiConsumer<String, String> setupHeaderFunc) {
    // API
    setupHeaderFunc.accept(HttpConstants.VENICE_API_VERSION, getVeniceApiVersionHeader());
    // Retry
    if (isRetryRequest()) {
      setupHeaderFunc.accept(HttpConstants.VENICE_RETRY, "1");
    }
    // Streaming
    if (chunkedResponse.isPresent()) {
      setupHeaderFunc.accept(HttpConstants.VENICE_STREAMING, "1");
    }
  }

  public HttpUriRequest composeRouterRequest(String storageNodeUri) {
    return composeRouterRequestInternal(storageNodeUri);
  }

  public RestRequest composeRestRequest(String storageNodeUri) {
    // set up header to pass map required by the Venice server
    HashMap<String, String> headerMap = new HashMap<>();
    setupVeniceHeaders(headerMap::put);

    String uri = storageNodeUri + getLocation();
    URI requestUri;

    try {
      requestUri = new URI(uri);
    } catch (URISyntaxException e) {
      throw new VeniceException("Failed to create URI for path " + uri, e);
    }

    RestRequestBuilder builder =
        new RestRequestBuilder(requestUri).setMethod(getHttpMethod().toString()).setHeaders(headerMap);
    setRestRequestEntity(builder);

    return builder.build();
  }

  public void setChunkedWriteHandler(
      ChannelHandlerContext ctx,
      VeniceChunkedWriteHandler chunkedWriteHandler,
      RouterStats<AggRouterHttpRequestStats> routerStats) {
    if (chunkedResponse.isPresent()) {
      // Defensive code
      throw new IllegalStateException("VeniceChunkedWriteHandler has already been setup");
    }
    this.chunkedResponse = Optional.of(new VeniceChunkedResponse(this, ctx, chunkedWriteHandler, routerStats));
  }

  public void setResponseDecompressor(VeniceResponseDecompressor decompressor) {
    if (responseDecompressor.isPresent()) {
      throw new VeniceException("VeniceResponseDecompressor has already been setup");
    }
    this.responseDecompressor = Optional.of(decompressor);
  }

  public VeniceResponseDecompressor getResponseDecompressor() {
    if (!responseDecompressor.isPresent()) {
      // Defensive code
      throw new IllegalStateException(
          "VeniceResponseDecompressor is not available for current request, and there must be a bug"
              + " when this exception happens.");
    }
    return responseDecompressor.get();
  }

  public Optional<VeniceChunkedResponse> getChunkedResponse() {
    return this.chunkedResponse;
  }

  public boolean isStreamingRequest() {
    return getChunkedResponse().isPresent();
  }

  public boolean isLongTailRetryAllowedForNewRoute() {
    return true;
  }

  public abstract RequestType getRequestType();

  public abstract VenicePath substitutePartitionKey(RouterKey s);

  public abstract VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s);

  public abstract HttpUriRequest composeRouterRequestInternal(String storageNodeUri);

  public abstract HttpMethod getHttpMethod();

  public abstract ByteBuf getRequestBody();

  public abstract Optional<byte[]> getBody();

  public abstract String getVeniceApiVersionHeader();

  public void setRestRequestEntity(RestRequestBuilder builder) {
  }
}
