package com.linkedin.venice.hadoop.input.recordreader.avro;

import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.utils.ArrayUtils;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IdentityVeniceRecordReaderTest {
  private static final IdentityVeniceRecordReader RECORD_READER = IdentityVeniceRecordReader.getInstance();
  private static final byte[] TEST_KEY_BYTES = "RANDOM_KEY".getBytes();
  private static final byte[] TEST_VALUE_BYTES = "RANDOM_VALUE".getBytes();

  @Test
  public void testGetKeyBytes() {
    byte[] extractedKey = RECORD_READER.getKeyBytes(ByteBuffer.wrap(TEST_KEY_BYTES), ByteBuffer.wrap(TEST_VALUE_BYTES));

    Assert.assertEquals(ArrayUtils.compareUnsigned(TEST_KEY_BYTES, extractedKey), 0);
  }

  @Test
  public void testGetValueBytes() {
    byte[] extractedValue =
        RECORD_READER.getValueBytes(ByteBuffer.wrap(TEST_KEY_BYTES), ByteBuffer.wrap(TEST_VALUE_BYTES));

    Assert.assertEquals(ArrayUtils.compareUnsigned(TEST_VALUE_BYTES, extractedValue), 0);
  }

  @Test
  public void testUnsupportedGetAvroData() {
    Assert.assertThrows(
        VeniceUnsupportedOperationException.class,
        () -> RECORD_READER.getAvroKey(ByteBuffer.wrap(TEST_KEY_BYTES), ByteBuffer.wrap(TEST_VALUE_BYTES)));
    Assert.assertThrows(
        VeniceUnsupportedOperationException.class,
        () -> RECORD_READER.getAvroValue(ByteBuffer.wrap(TEST_KEY_BYTES), ByteBuffer.wrap(TEST_VALUE_BYTES)));
  }
}
