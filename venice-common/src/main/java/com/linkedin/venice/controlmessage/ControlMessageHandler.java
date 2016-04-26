package com.linkedin.venice.controlmessage;

/**
 * Handler to deal with control message.
 */
public interface ControlMessageHandler<T extends ControlMessage> {
  void handleMessage(T message);
}
