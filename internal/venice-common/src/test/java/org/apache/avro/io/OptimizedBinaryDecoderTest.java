package org.apache.avro.io;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.utils.DataProviderUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OptimizedBinaryDecoderTest {
  private static final OptimizedBinaryDecoderFactory FACTORY = OptimizedBinaryDecoderFactory.defaultFactory();

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testHappyPath(boolean buffered) throws IOException {
    BinaryEncoder encoder = testHappyPath(null, buffered, false);
    testHappyPath(encoder, buffered, false);
    testHappyPath(encoder, buffered, true);
  }

  private BinaryEncoder testHappyPath(
      BinaryEncoder reusedEncoder,
      boolean bufferedEncoder,
      boolean useByteBufferConstructor) throws IOException {
    int wholeArraySize = 100;
    int byteBufferSize = 50;
    byte[] bytes = new byte[byteBufferSize];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    Random random = ThreadLocalRandom.current();
    random.nextBytes(bytes);
    float randomFloat1 = random.nextFloat();
    float randomFloat2 = random.nextFloat();

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(wholeArraySize);
    BinaryEncoder encoder =
        AvroCompatibilityHelper.newBinaryEncoder(byteArrayOutputStream, bufferedEncoder, reusedEncoder);
    encoder.writeFloat(randomFloat1);
    encoder.writeBytes(byteBuffer);
    encoder.writeFloat(randomFloat2);
    encoder.flush();
    byte[] wholeArray = byteArrayOutputStream.toByteArray();

    BinaryDecoder decoder;
    if (useByteBufferConstructor) {
      decoder = FACTORY.createOptimizedBinaryDecoder(ByteBuffer.wrap(wholeArray));
    } else {
      decoder = FACTORY.createOptimizedBinaryDecoder(wholeArray, 0, wholeArray.length);
    }
    float deserializedFloat1 = decoder.readFloat();
    ByteBuffer deserializedByteBuffer = decoder.readBytes(null);
    float deserializedFloat2 = decoder.readFloat();

    String assertionMessagePrefix = bufferedEncoder ? "Buffered encoder. " : "Non-Buffered encoder. ";
    assertionMessagePrefix += reusedEncoder == null ? "First run: " : "Reused encoder: ";
    Assert.assertEquals(
        deserializedFloat1,
        randomFloat1,
        assertionMessagePrefix + "The deserialized float 1 does not have the expected value");
    Assert.assertEquals(
        deserializedFloat2,
        randomFloat2,
        assertionMessagePrefix + "The deserialized float 2 does not have the expected value");
    Assert.assertEquals(
        deserializedByteBuffer.array(),
        wholeArray,
        assertionMessagePrefix
            + "The deserializedByteBuffer should be backed by the same wholeArray instance as was passed to the decoder");
    Assert.assertEquals(
        deserializedByteBuffer.position(),
        Float.BYTES + 1,
        assertionMessagePrefix + "The position of the deserializedByteBuffer should be offset");
    Assert.assertEquals(
        byteBuffer.limit(),
        byteBufferSize,
        assertionMessagePrefix + "The limit of the deserializedByteBuffer should be equal byteBufferSize");
    Assert.assertEquals(
        byteBuffer,
        deserializedByteBuffer,
        assertionMessagePrefix + "The content of the deserialized byte buffer is not as expected");

    return encoder;
  }

  @Test
  public void testBadInitFailsWithProperErrorMessage() throws IOException {
    byte[] wholeArray1 = new byte[100];

    /**
     * There should be no code that directly instantiates the decoder, besides the {@link OptimizedBinaryDecoderFactory}
     * because otherwise they risk calling {@link BinaryDecoder#configure(byte[], int, int)} without then calling
     * {@link OptimizedBinaryDecoder#configureByteBuffer(byte[], int, int)}. In this test, we check that we have
     * proper defensive code to catch this improper usage...
     */
    OptimizedBinaryDecoder decoder = new OptimizedBinaryDecoder();
    decoder.configure(wholeArray1, 0, wholeArray1.length);
    Assert.assertThrows(IllegalStateException.class, () -> decoder.readFloat());
  }
}
