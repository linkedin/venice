package com.linkedin.venice.status;

import java.io.IOException;


/**
 * Channel used to send and receive control message.
 */
public interface StatusMessageChannel {
  /**
   * Send message to controller.
   *
   * @param message
   *
   * @throws IOException Met any errors when sending the meesage through network.
   */
  public void sendToController(StatusMessage message)
      throws IOException;

  //TODO we only need send to controller now. Will add send to storage nodes in the further.
  //public void sendToStorageNodes(StatusMessage message, List<Instance> instances);

  /**
   * Register a handler to handle a specific message type.
   *
   * @param clazz
   * @param handler
   * @param <T>
   */
  public <T extends StatusMessage> void registerHandler(Class<T> clazz, StatusMessageHandler<T> handler);

  /**
   * Remove a handler for a specific message type.
   *
   * @param clazz
   * @param handler
   * @param <T>
   */
  public <T extends StatusMessage> void unRegisterHandler(Class<T> clazz, StatusMessageHandler<T> handler);
}
