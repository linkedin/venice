package org.apache.avro.io;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.DataProviderUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OptimizedBinaryDecoderTest {
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHappyPath(boolean buffered) throws IOException {
    BinaryEncoder encoder = testHappyPath(null, buffered);
    testHappyPath(encoder, buffered);
  }

  private BinaryEncoder testHappyPath(BinaryEncoder reusedEncoder, boolean bufferedEncoder) throws IOException {
    int wholeArraySize = 100;
    int byteBufferSize = 50;
    byte[] bytes = new byte[byteBufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    Random random = new Random();
    random.nextBytes(bytes);
    float randomFloat1 = random.nextFloat();
    float randomFloat2 = random.nextFloat();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(wholeArraySize);
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream, bufferedEncoder, reusedEncoder);
    encoder.writeFloat(randomFloat1);
    encoder.writeBytes(byteBuffer);
    encoder.writeFloat(randomFloat2);
    encoder.flush();
    byte[] wholeArray = byteArrayOutputStream.toByteArray();

    BinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(wholeArray, 0, wholeArray.length);
    float deserializedFloat1 = decoder.readFloat();
    ByteBuffer deserializedByteBuffer = decoder.readBytes(null);
    float deserializedFloat2 = decoder.readFloat();

    String assertionMessagePrefix = bufferedEncoder
        ? "Buffered encoder. "
        : "Non-Buffered encoder. ";
    assertionMessagePrefix += null == reusedEncoder
        ? "First run: "
        : "Reused encoder: ";
    Assert.assertEquals(deserializedFloat1, randomFloat1,
        assertionMessagePrefix + "The deserialized float 1 does not have the expected value");
    Assert.assertEquals(deserializedFloat2, randomFloat2,
        assertionMessagePrefix + "The deserialized float 2 does not have the expected value");
    Assert.assertEquals(deserializedByteBuffer.array(), wholeArray,
        assertionMessagePrefix + "The deserializedByteBuffer should be backed by the same wholeArray instance as was passed to the decoder");
    Assert.assertEquals(deserializedByteBuffer.position(), Float.BYTES + 1,
        assertionMessagePrefix + "The position of the deserializedByteBuffer should be offset");
    Assert.assertEquals(byteBuffer.limit(), byteBufferSize,
        assertionMessagePrefix + "The limit of the deserializedByteBuffer should be equal byteBufferSize");
    Assert.assertEquals(byteBuffer, deserializedByteBuffer,
        assertionMessagePrefix + "The content of the deserialized byte buffer is not as expected");

    return encoder;
  }

  @Test
  public void testBadInitFailsWithProperErrorMessage() throws IOException {
    byte[] wholeArray1 = new byte[100];
    byte[] wholeArray2 = new byte[100];
    OptimizedBinaryDecoder decoder = OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(wholeArray1, 0, wholeArray1.length);
    decoder.configure(wholeArray2, 0 , wholeArray2.length);
    try {
      decoder.readFloat();
      Assert.fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException e) {
      // expected
    }

    decoder = OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(wholeArray1, 0, wholeArray1.length);
    decoder.configureByteBuffer(wholeArray2, 0 , wholeArray2.length);
    try {
      decoder.readFloat();
      Assert.fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException e) {
      // expected
    }
  }
}
