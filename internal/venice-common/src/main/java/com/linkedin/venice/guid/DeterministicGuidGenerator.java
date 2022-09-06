package com.linkedin.venice.guid;

import com.linkedin.venice.kafka.protocol.GUID;
import java.nio.ByteBuffer;


public class DeterministicGuidGenerator implements GuidGenerator {
  final GUID guid = new GUID();

  public DeterministicGuidGenerator(long prefix, long suffix) {
    byte[] guidBytes = ByteBuffer.allocate(16).putLong(prefix).putLong(suffix).array();
    guid.bytes(guidBytes);
  }

  @Override
  public GUID getGuid() {
    return guid;
  }
}
