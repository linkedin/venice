package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.UUID;


/**
 * A Guid generator which internally uses {@link UUID#randomUUID()}.
 *
 * @see GuidUtils#getGUID(VeniceProperties)
 */
public class JavaUtilGuidV4Generator implements GuidGenerator {
  /**
   * Required in order to construct via reflection.
   */
  public JavaUtilGuidV4Generator() {
  }

  @Override
  public GUID getGuid() {
    UUID javaUtilUuid = UUID.randomUUID();
    GUID guid = new GUID();
    byte[] guidBytes = ByteBuffer.allocate(16)
        .putLong(javaUtilUuid.getMostSignificantBits())
        .putLong(javaUtilUuid.getLeastSignificantBits())
        .array();
    guid.bytes(guidBytes);
    return guid;
  }
}
