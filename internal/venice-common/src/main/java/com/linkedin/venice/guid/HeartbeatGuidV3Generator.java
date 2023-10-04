package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * A Guid generator which uses the {@link java.util.UUID#nameUUIDFromBytes(byte[])}. Which is a type 3 Guid that will
 * not collide with our type 4 Guid generated for user data using {@link JavaUtilGuidV4Generator}
 */
public class HeartbeatGuidV3Generator implements GuidGenerator {
  private static HeartbeatGuidV3Generator instance;

  private HeartbeatGuidV3Generator() {
  }

  public static synchronized HeartbeatGuidV3Generator getInstance() {
    if (instance == null) {
      instance = new HeartbeatGuidV3Generator();
    }
    return instance;
  }

  @Override
  public GUID getGuid() {
    UUID javaUtilUuid = UUID.nameUUIDFromBytes("heartbeatControlMessage".getBytes());
    GUID guid = new GUID();
    byte[] guidBytes = ByteBuffer.allocate(16)
        .putLong(javaUtilUuid.getMostSignificantBits())
        .putLong(javaUtilUuid.getLeastSignificantBits())
        .array();
    guid.bytes(guidBytes);
    return guid;
  }
}
