package com.linkedin.venice.controlmessage;

import java.util.Map;


/**
 * Venice control message which is used to transfer command and status between storage node and controller, so that
 * controller could control the whole cluster and make the global decision.
 */
public interface ControlMessage {
  /**
   * Get Id of message.
   * @return
   */
  public String getMessageId();

  /**
   * Get K-V paris of all the fields in message.
   *
   * @return
   */
  public Map<String, String> getFields();

  /**
   * Build message from the given fields.
   *
   * @param fields
   */
  public void buildFromFields(Map<String, String> fields);
}
