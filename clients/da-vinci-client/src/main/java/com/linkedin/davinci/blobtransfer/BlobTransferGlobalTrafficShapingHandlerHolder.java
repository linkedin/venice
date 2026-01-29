package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.lazy.Lazy;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.util.concurrent.Executors;


/**
 * Singleton holder for the GlobalChannelTrafficShapingHandler.
 * This class provides a thread-safe way to access a single instance of the global traffic shaping handler
 * that can be shared across all network channels of blob transfer in both server and client side to enforce global rate limits.
 */
public class BlobTransferGlobalTrafficShapingHandlerHolder {
  private static final long CHECK_INTERVAL_MS = 1000L; // traffic shaping checks every 1 second
  private static final long DEFAULT_READ_CHANNEL_LIMIT = 0; // unlimited channels by default
  private static final long DEFAULT_WRITE_CHANNEL_LIMIT = 0; // unlimited channels by default

  private static volatile long readLimit = 0; // default unlimited, will update based on the config setting.
  private static volatile long writeLimit = 0; // default unlimited, will update based on the config setting.

  private static final Lazy<GlobalChannelTrafficShapingHandler> GLOBAL_CHANNEL_TRAFFIC_SHAPING_HANDLER = Lazy.of(() -> {
    return new GlobalChannelTrafficShapingHandler(
        Executors.newScheduledThreadPool(1, new DaemonThreadFactory("blob-transfer-global-traffic-shaper")),
        writeLimit, // writeGlobalLimit
        readLimit, // readGlobalLimit
        DEFAULT_WRITE_CHANNEL_LIMIT,
        DEFAULT_READ_CHANNEL_LIMIT,
        CHECK_INTERVAL_MS);
  });

  private BlobTransferGlobalTrafficShapingHandlerHolder() {
  }

  /**
   * Gets the singleton instance of GlobalChannelTrafficShapingHandler.
   * Handler is lazily created on first access.
   * @param readLimit maximum number of bytes to read per second
   * @param writeLimit maximum number of bytes to write per second
   * @return the global traffic shaping handler
   */
  public static GlobalChannelTrafficShapingHandler getGlobalChannelTrafficShapingHandlerInstance(
      long readLimit,
      long writeLimit) {
    // Set limits before getting the instance to ensure they're used during initialization
    BlobTransferGlobalTrafficShapingHandlerHolder.readLimit = readLimit;
    BlobTransferGlobalTrafficShapingHandlerHolder.writeLimit = writeLimit;
    GlobalChannelTrafficShapingHandler handler = GLOBAL_CHANNEL_TRAFFIC_SHAPING_HANDLER.get();

    return handler;
  }
}
