package com.linkedin.venice.status;

/**
 * Handler to deal with incoming {@link StatusMessage} instances.
 */
public interface StatusMessageHandler<T extends StatusMessage> {
  void handleMessage(T message);
}
