package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.router.api.ResourcePath;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.VeniceResponseDecompressor;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.internal.ConcurrentSet;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpUriRequest;


public abstract class VenicePath implements ResourcePath<RouterKey> {
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
  private Set<String> slowStorageNodeSet = new ConcurrentSet<>();
  // Whether the request supports streaming or not
  private Optional<VeniceChunkedResponse> chunkedResponse = Optional.empty();
  // Response decompressor
  private Optional<VeniceResponseDecompressor> responseDecompressor = Optional.empty();


  public VenicePath(String resourceName, boolean smartLongTailRetryEnabled, int smartLongTailRetryAbortThresholdMs) {
    this(resourceName, smartLongTailRetryEnabled, smartLongTailRetryAbortThresholdMs, new SystemTime());
  }

  public VenicePath(String resourceName, boolean smartLongTailRetryEnabled, int smartLongTailRetryAbortThresholdMs, Time time) {
    this.resourceName = resourceName;
    this.storeName = Version.parseStoreFromKafkaTopicName(resourceName);
    this.versionNumber = Version.parseVersionFromKafkaTopicName(resourceName);
    this.smartLongTailRetryEnabled = smartLongTailRetryEnabled;
    this.smartLongTailRetryAbortThresholdMs = smartLongTailRetryAbortThresholdMs;
    this.time = time;
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
    final int[] size = {0};
    getPartitionKeys().stream().forEach(key -> size[0] += key.getKeyBuffer().remaining());

    return size[0];
  }

  public int getVersionNumber(){
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

  private void addSlowStorageNode(String slowStorageNode) {
    slowStorageNodeSet.add(slowStorageNode);
  }

  private boolean isStorageNodeSlow(String storageNode) {
    return slowStorageNodeSet.contains(storageNode);
  }

  /**
   * This function is used to check whether Router could send retry request to the specified storage node.
   * It will return false if the requested storage node has been marked as slow.
   *
   * If the current request is not a retry request, this function will add the passed storage node to
   * the slow storage node set.
   *
   * @param storageNode
   * @return
   */
  public boolean canRequestStorageNode(String storageNode) {
    if (!smartLongTailRetryEnabled) {
      return true;
    }
    if (isRetryRequest()) {
      if (isStorageNodeSlow(storageNode)) {
        return false;
      }
    } else {
      addSlowStorageNode(storageNode);
    }
    return true;
  }

  public void recordOriginalRequestStartTimestamp() {
    if (! isRetryRequest()) {
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
    HttpUriRequest request = composeRouterRequestInternal(storageNodeUri);
    setupVeniceHeaders( (k, v) -> request.setHeader(k, v));

    return request;
  }

  public void setChunkedWriteHandler(ChannelHandlerContext ctx, VeniceChunkedWriteHandler chunkedWriteHandler,
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
      throw new IllegalStateException("VeniceResponseDecompressor is not available for current request, and there must be a bug"
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

  public void markStorageNodeAsFast(String fastStorageNode) {
    slowStorageNodeSet.remove(fastStorageNode);
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

  public abstract String getVeniceApiVersionHeader();
}
