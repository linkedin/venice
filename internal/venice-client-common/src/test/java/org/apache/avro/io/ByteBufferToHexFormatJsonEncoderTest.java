package org.apache.avro.io;

import com.linkedin.venice.kafka.protocol.GUID;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ByteBufferToHexFormatJsonEncoderTest {
  @Test
  public void testWriteFixed() throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ByteBufferToHexFormatJsonEncoder encoder = new ByteBufferToHexFormatJsonEncoder(GUID.SCHEMA$, outputStream);
    GUID guid = new GUID(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
    encoder.writeFixed(guid.bytes());
    encoder.flush();
    Assert.assertEquals(new String(outputStream.toByteArray()), "\"0102030405060708090a0b0c0d0e0f10\"");
  }
}
