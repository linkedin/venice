package com.linkedin.venice.controlmessage;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;


/**
 * Venice control message which is used to transfer command and status between storage node and controller, so that
 * controller could control the whole cluster and make the global decision.
 */
public abstract class ControlMessage {
  /**
   * Create message from given fields map. Should be override in each sub-class.
   *
   * @param fields
   */
  public ControlMessage(Map<String, String> fields) {
  }

  /**
   * Default constructor, only used in constructor of sub-class. So visible level is procted but not pulic.
   */
  protected ControlMessage() {

  }

  /**
   * Get Id of message.
   *
   * @return
   */
  public abstract String getMessageId();

  /**
   * Get K-V paris of all the fields in message.
   *
   * @return
   */
  public abstract Map<String, String> getFields();

  protected String getRequiredField(Map<String, String> fields, String key) {
    if (fields.containsKey(key)) {
      return fields.get(key);
    } else {
      throw new VeniceException("Missing required field:" + key + " when building control message.");
    }
  }

  protected String getOptionalField(Map<String, String> fields, String key) {
    if (fields.containsKey(key)) {
      return fields.get(key);
    } else {
      return null;
    }
  }

}
