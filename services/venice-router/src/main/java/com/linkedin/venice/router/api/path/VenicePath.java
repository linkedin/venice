package com.linkedin.venice.router.api.path;

import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.meta.StoreVersionName;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.RouterRetryConfig;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VeniceResponseDecompressor;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.client.methods.HttpUriRequest;


public abstract class VenicePath implements ResourcePath<RouterKey> {
  protected final StoreVersionName storeVersionName;
  private Collection<RouterKey> partitionKeys;
  protected final RouterRetryConfig retryConfig;
  protected final RetryManager retryManager;
  private final VeniceResponseDecompressor responseDecompressor;
  private boolean retryRequest = false;
  private long originalRequestStartTs = -1;
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
  private boolean ignoreSlowStorageNodes = false;

  public VenicePath(
      StoreVersionName storeVersionName,
      RouterRetryConfig retryConfig,
      RetryManager retryManager,
      VeniceResponseDecompressor responseDecompressor) {
    this.storeVersionName = storeVersionName;
    this.retryConfig = retryConfig;
    this.retryManager = retryManager;
    this.responseDecompressor = responseDecompressor;
  }

  public synchronized long getRequestId() {
    return -1;
  }

  public int getHelixGroupId() {
    return -1;
  }

  public void setHelixGroupId(int helixGroupId) {
    // No-op.
  }

  public boolean isSmartLongTailRetryEnabled() {
    return false;
  }

  public int getSmartLongTailRetryAbortThresholdMs() {
    return -1;
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
    int size = 0;
    for (RouterKey key: getPartitionKeys()) {
      size += key.getKeyBuffer().remaining();
    }
    return size;
  }

  public int getVersionNumber() {
    return this.storeVersionName.getVersionNumber();
  }

  @Nonnull
  @Override
  public String getResourceName() {
    return this.storeVersionName.getName();
  }

  public String getStoreName() {
    return this.storeVersionName.getStoreName();
  }

  @Override
  public void setRetryRequest() {
    this.retryRequest = true;
  }

  @Override
  public void setRetryRequest(HttpResponseStatus hrs) {
    this.retryRequest = true;
    if (hrs.code() >= 500) {
      // ignore slow storage nodes for retry requests with 5xx status code
      this.ignoreSlowStorageNodes = true;
    }
  }

  protected void setupRetryRelatedInfo(VenicePath originalPath) {
    if (originalPath.isRetryRequest()) {
      setRetryRequest();
    }

    /**
     * All the sub-requests and retry requests for a multi-get request will share the same slow
     * storage node set.
     */
    slowStorageNodeSet = originalPath.slowStorageNodeSet;
    ignoreSlowStorageNodes = originalPath.ignoreSlowStorageNodes;
    setOriginalRequestStartTs(originalPath.getOriginalRequestStartTs());
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

  public abstract int getLongTailRetryThresholdMs();

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
    if (!isSmartLongTailRetryEnabled()) {
      return true;
    }
    return !isRetryRequest() || // original request
        ignoreSlowStorageNodes || // retry request but ignore slowNodeSet
        !slowStorageNodeSet.contains(storageNode); // retry request and not in slowNodeSet
  }

  public void recordOriginalRequestStartTimestamp() {
    if (!isRetryRequest()) {
      setOriginalRequestStartTs(getTime().getMilliseconds());
    }
  }

  /**
   * This function will check whether the retry request already passed the retry delay threshold.
   * If yes, return true.
   * @return
   */
  public boolean isRetryRequestTooLate() {
    if (!isSmartLongTailRetryEnabled()) {
      return false;
    }
    if (isRetryRequest()) {
      // Retry request
      long retryDelay = getTime().getMilliseconds() - getOriginalRequestStartTs();
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
    if (isStreamingRequest()) {
      setupHeaderFunc.accept(HttpConstants.VENICE_STREAMING, "1");
    }
  }

  public HttpUriRequest composeRouterRequest(String storageNodeUri) {
    return composeRouterRequestInternal(storageNodeUri);
  }

  public void setChunkedWriteHandler(
      ChannelHandlerContext ctx,
      VeniceChunkedWriteHandler chunkedWriteHandler,
      RouterStats<AggRouterHttpRequestStats> routerStats) {
    throw new UnsupportedOperationException(
        "setChunkedWriteHandler is only available with " + VeniceMultiKeyPath.class.getSimpleName() + " subclasses.");
  }

  public @Nullable String getClientComputeHeader() {
    return null;
  }

  public VeniceResponseDecompressor getResponseDecompressor() {
    if (responseDecompressor == null) {
      // Defensive code
      throw new IllegalStateException(
          "VeniceResponseDecompressor is not available for current request, and there must be a bug"
              + " when this exception happens.");
    }
    return responseDecompressor;
  }

  public @Nullable VeniceChunkedResponse getChunkedResponse() {
    return null;
  }

  public boolean isStreamingRequest() {
    return getChunkedResponse() != null;
  }

  public boolean isLongTailRetryAllowedForNewRequest() {
    return true;
  }

  public abstract RequestType getRequestType();

  protected RequestType getStreamingRequestType() {
    throw new IllegalStateException("This should not be called on " + this.getClass().getSimpleName());
  }

  public abstract VenicePath substitutePartitionKey(RouterKey s);

  public abstract VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s);

  public abstract HttpUriRequest composeRouterRequestInternal(String storageNodeUri);

  public abstract HttpMethod getHttpMethod();

  public abstract byte[] getBody();

  public abstract String getVeniceApiVersionHeader();

  public void recordRequest() {
    retryManager.recordRequest();
  }

  public boolean isLongTailRetryWithinBudget(int numberOfRoutes) {
    return retryManager.isRetryAllowed(numberOfRoutes);
  }

  protected Time getTime() {
    return SystemTime.INSTANCE;
  }
}
