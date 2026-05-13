package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;


/**
 * A GUID generator for Leader Step-Down Stamp control messages.
 *
 * <p>Uses {@link java.util.UUID#nameUUIDFromBytes(byte[])} to generate a type 3 GUID that will
 * not collide with type 4 GUIDs generated for user data using {@link JavaUtilGuidV4Generator},
 * nor with the type-3 GUIDs used for heartbeats and Declaration of Leadership stamps.
 *
 * <p>This GUID identifies the Leader Step-Down message <b>type</b> (not individual messages).
 * All Leader Step-Down stamps share the same GUID, which allows receivers to distinguish them
 * from data, heartbeats, and DoL stamps. Individual stamps are distinguished by their payload
 * (specifically the {@code LeaderMetadata.termId} that identifies the term being closed).
 */
public class LeaderStepDownStampGuidGenerator implements GuidGenerator {
  private static LeaderStepDownStampGuidGenerator instance;
  private static final GUID LEADER_STEPDOWN_STAMP_GUID;

  static {
    UUID javaUtilUuid = UUID.nameUUIDFromBytes("leaderStepDownStampControlMessage".getBytes(StandardCharsets.UTF_8));
    LEADER_STEPDOWN_STAMP_GUID = new GUID();
    byte[] guidBytes = ByteBuffer.allocate(16)
        .putLong(javaUtilUuid.getMostSignificantBits())
        .putLong(javaUtilUuid.getLeastSignificantBits())
        .array();
    LEADER_STEPDOWN_STAMP_GUID.bytes(guidBytes);
  }

  private LeaderStepDownStampGuidGenerator() {
  }

  public static synchronized LeaderStepDownStampGuidGenerator getInstance() {
    if (instance == null) {
      instance = new LeaderStepDownStampGuidGenerator();
    }
    return instance;
  }

  @Override
  public GUID getGuid() {
    return LEADER_STEPDOWN_STAMP_GUID;
  }
}
