package com.linkedin.venice.listener;

import com.linkedin.davinci.listener.response.ReadResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;


/**
 * Callback interface to handle the response from the {@link StorageReadRequestHandler#processRequest} method.
 */
public interface StorageResponseHandlerCallback {
  void onReadResponse(ReadResponse readResponse);

  /**
   * Use this API to send error responses to the client.
   * @param readResponseStatus the status of the response
   * @param message the message to send to the client
   */
  void onError(VeniceReadResponseStatus readResponseStatus, String message);
}
