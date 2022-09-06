package com.linkedin.venice.status;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import java.util.HashMap;
import java.util.Map;


/**
 * Venice control message which is used to transfer command and status between storage node and controller, so that
 * controller could control the whole cluster and make the global decision.
 */
public abstract class StatusMessage {

  /* DO NOT CHANGE the field names. The field names are used
  in serialization/de-serialization across two processes
  storage node and controller. If the field names are removed
  or modified, it will cause serialization, de-serialization
  issues.
  
  Adding fields and when not available doing the default thing
  should be fine.
   */
  private static final String MESSAGE_ID = "messageId";
  protected final String messageId;

  /**
   * Create message from given fields map. Should be override in each sub-class.
   *
   * @param fields
   */
  protected StatusMessage(Map<String, String> fields) {
    this.messageId = getRequiredField(fields, MESSAGE_ID);
  }

  /**
   * Default constructor, only used in constructor of sub-class. So visible level is protected but not public.
   */
  protected StatusMessage() {
    messageId = generateMessageId();
  }

  /**
   * Get Id of message.
   * @return
   */
  public final String getMessageId() {
    return this.messageId;
  }

  /**
   * Get K-V paris of all the fields in message.
   *
   * @return
   */
  public Map<String, String> getFields() {
    Map<String, String> fields = new HashMap<>();
    fields.put(MESSAGE_ID, messageId);
    return fields;
  }

  protected static String getRequiredField(Map<String, String> fields, String key) {
    if (fields.containsKey(key)) {
      return fields.get(key);
    } else {
      throw new VeniceException("Missing required field:" + key + " when building control message.");
    }
  }

  protected static String getOptionalField(Map<String, String> fields, String key) {
    if (fields.containsKey(key)) {
      return fields.get(key);
    } else {
      return null;
    }
  }

  public static String generateMessageId() {
    // Confirmed with helix team. The message Id is used as the key for zk node. So it must be a global unique Id.
    // And Helix also use Java UUID for other helix message. So we just follow this stand here.
    return GuidUtils.getGUIDString();
  }

}
