package com.linkedin.venice.controlmessage;

/**
 * Handler to deal with control message.
 */
public interface ControlMessageHandler<T extends ControlMessage> {
  public void handleMessage(T message);
}
