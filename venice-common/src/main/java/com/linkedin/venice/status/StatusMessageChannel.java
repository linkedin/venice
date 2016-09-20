package com.linkedin.venice.status;

import java.io.IOException;


/**
 * Channel used to send and receive control message.
 */
public interface StatusMessageChannel {
  /**
   * Send message to controller. If met any error during the seding, retry it after waiting @retryDurationMs ms.
   *
   * @param message
   * @param retryCount      retry how many times.
   * @param retryDurationMs the duration between two retries.
   *
   * @throws com.linkedin.venice.exceptions.VeniceException Met any errors when sending the message through network.
   */
  public void sendToController(StatusMessage message, int retryCount, long retryDurationMs);

  /**
   * Send message to controller.
   *
   * @throws com.linkedin.venice.exceptions.VeniceException Met any errors when sending the message through network.
   */
  public void sendToController(StatusMessage message);

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
