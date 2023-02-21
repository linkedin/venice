package com.linkedin.venice.hadoop;

import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;


/**
 * Created a new class as mocking readDictionaryFromKafka on an actual
 * {@link VeniceAvroMapper} object seemed to be not working.
 */
public class TestVeniceAvroMapperClass extends VeniceAvroMapper {
  @Override
  protected ByteBuffer readDictionaryFromKafka(String topicName, VeniceProperties props) {
    return ByteBuffer.wrap("TEST_DICT".getBytes());
  }
}
