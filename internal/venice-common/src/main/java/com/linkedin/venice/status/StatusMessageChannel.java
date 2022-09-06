package com.linkedin.venice.status;

/**
 * Channel used to send and receive control message.
 */
public interface StatusMessageChannel {
  /**
   * Send message to controller. If met any error during the sending, retry it after retryDurationMs until retry @retryCount times.
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

  /**
   * Send message to all storage nodes in the given cluster. If met any error during the sending, retry it
   * after retryDurationMs until retry @retryCount times.
   */
  public void sendToStorageNodes(String clusterName, StatusMessage message, String resourceName, int retryCount);

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
