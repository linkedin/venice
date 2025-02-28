package com.linkedin.davinci.blobtransfer;

import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Singleton holder for the GlobalChannelTrafficShapingHandler.
 * This class provides a thread-safe way to access a single instance of the global traffic shaping handler
 * that can be shared across all network channels of blob transfer in both server and client side to enforce global rate limits.
 */
public class BlobTransferGlobalTrafficShapingHandlerHolder {
  private static final AtomicReference<GlobalChannelTrafficShapingHandler> GLOBAL_CHANNEL_TRAFFIC_SHAPING_HANDLER =
      new AtomicReference<>();
  private static final long CHECK_INTERVAL_MS = 1000L; // traffic shaping checks every 1 second
  private static final long DEFAULT_READ_CHANNEL_LIMIT = 0; // unlimited channels by default
  private static final long DEFAULT_WRITE_CHANNEL_LIMIT = 0; // unlimited channels by default

  private BlobTransferGlobalTrafficShapingHandlerHolder() {
  }

  /**
   * Gets or creates the singleton instance of GlobalChannelTrafficShapingHandler.
   *
   * @param readLimit maximum number of bytes to read per second
   * @param writeLimit maximum number of bytes to write per second
   * @return the global traffic shaping handler
   */
  public static GlobalChannelTrafficShapingHandler getOrCreate(long readLimit, long writeLimit) {
    GlobalChannelTrafficShapingHandler handler = GLOBAL_CHANNEL_TRAFFIC_SHAPING_HANDLER.get();
    if (handler == null) {
      handler = new GlobalChannelTrafficShapingHandler(Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r, "blob-transfer-global-traffic-shaper");
        thread.setDaemon(true);
        return thread;
      }),
          writeLimit, // writeGlobalLimit
          readLimit, // readGlobalLimit
          DEFAULT_WRITE_CHANNEL_LIMIT,
          DEFAULT_READ_CHANNEL_LIMIT,
          CHECK_INTERVAL_MS);

      if (GLOBAL_CHANNEL_TRAFFIC_SHAPING_HANDLER.compareAndSet(null, handler)) {
        return handler;
      } else {
        // Another thread created the handler first, close this one and use the existing one
        handler.release();
        return GLOBAL_CHANNEL_TRAFFIC_SHAPING_HANDLER.get();
      }
    }
    return handler;
  }
}
