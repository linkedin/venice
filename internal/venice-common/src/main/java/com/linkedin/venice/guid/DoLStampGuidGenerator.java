package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;


/**
 * A GUID generator for Declaration of Leadership (DoL) control messages.
 *
 * <p>Uses {@link java.util.UUID#nameUUIDFromBytes(byte[])} to generate a type 3 GUID that will
 * not collide with type 4 GUIDs generated for user data using {@link JavaUtilGuidV4Generator}.
 *
 * <p>This GUID identifies the DoL message <b>type</b> (not individual messages). All DoL messages
 * share the same GUID, which allows receivers to distinguish DoL control messages from other
 * message types. Individual DoL messages are distinguished by their payload content (leadership
 * term, host ID, etc.), not by the GUID.
 */
public class DoLStampGuidGenerator implements GuidGenerator {
  private static DoLStampGuidGenerator instance;
  private static final GUID DOL_STAMP_GUID;

  static {
    UUID javaUtilUuid =
        UUID.nameUUIDFromBytes("declarationOfLeadershipControlMessage".getBytes(StandardCharsets.UTF_8));
    DOL_STAMP_GUID = new GUID();
    byte[] guidBytes = ByteBuffer.allocate(16)
        .putLong(javaUtilUuid.getMostSignificantBits())
        .putLong(javaUtilUuid.getLeastSignificantBits())
        .array();
    DOL_STAMP_GUID.bytes(guidBytes);
  }

  private DoLStampGuidGenerator() {
  }

  public static synchronized DoLStampGuidGenerator getInstance() {
    if (instance == null) {
      instance = new DoLStampGuidGenerator();
    }
    return instance;
  }

  @Override
  public GUID getGuid() {
    return DOL_STAMP_GUID;
  }
}
