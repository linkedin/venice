package com.linkedin.venice.client.store.transport;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;


/**
 * Callback to support streaming in {@link TransportClient}
 */
public interface TransportClientStreamingCallback {
  /**
   * This will be invoked when headers are available.
   * @param headers
   */
  void onHeaderReceived(Map<String, String> headers);

  /**
   * This will be invoked when a new data chunk is available.
   * @param chunk
   */
  void onDataReceived(ByteBuffer chunk);

  /**
   * This will be invoked when the response is fully completed.
   * When any error happens, {@param exception} will contain the underlying exception.
   * @param exception
   */
  void onCompletion(Optional<VeniceClientException> exception);
}
