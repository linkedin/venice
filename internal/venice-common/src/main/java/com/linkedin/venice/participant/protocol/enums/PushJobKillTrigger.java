package com.linkedin.venice.participant.protocol.enums;

import java.util.HashMap;
import java.util.Map;


/**
 * Represents the trigger that caused an offline push job to be terminated.
 */
public enum PushJobKillTrigger {
  // User/system-initiated actions
  USER_REQUEST(0), // Push job was killed due to explicit user request
  VERSION_RETIREMENT(1), // Push job was killed due to version retirement

  // System-detected violations or preemption
  SLA_VIOLATION(2), // Push job violated SLA (e.g., took too long)
  PREEMPTED_BY_FULL_PUSH(3), // Repush was preempted by a new full/batch push

  // Failures during ingestion or version setup
  INGESTION_FAILURE(4), // Push job failed due to ingestion errors
  VERSION_CREATION_FAILURE(5), // Push job failed due to version creation errors
  PUSH_JOB_FAILED(6), // Generic failure during push job

  // Cleanup and maintenance triggers
  LINGERING_VERSION_TOPIC(7), // Push job was killed due to lingering version topic

  // Catch-all
  UNKNOWN(8); // Unknown reason

  private final int code;

  private static final Map<Integer, PushJobKillTrigger> CODE_MAP = new HashMap<>(8);
  private static final Map<String, PushJobKillTrigger> NAME_MAP = new HashMap<>(8);

  static {
    for (PushJobKillTrigger trigger: values()) {
      CODE_MAP.put(trigger.code, trigger);
      NAME_MAP.put(trigger.name().toLowerCase(), trigger);
    }
  }

  PushJobKillTrigger(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  /**
   * Returns the enum constant for the given code.
   *
   * @param code the integer code
   * @return the corresponding KillPushJobTrigger, or null if not found
   */
  public static PushJobKillTrigger fromCode(int code) {
    return CODE_MAP.get(code);
  }

  /**
   * Returns the enum constant for the given name (case-insensitive).
   *
   * @param name the name of the enum constant
   * @return the corresponding KillPushJobTrigger, or null if not found
   */
  public static PushJobKillTrigger fromString(String name) {
    if (name == null) {
      return null;
    }
    return NAME_MAP.get(name.toLowerCase());
  }
}
