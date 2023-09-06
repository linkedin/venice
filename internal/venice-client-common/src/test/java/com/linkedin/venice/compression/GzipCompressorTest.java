package com.linkedin.venice.compression;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GzipCompressorTest {
  @Test
  public void testCompressWhenByteBufferIsBackedByArray() throws IOException {
    try (GzipCompressor compressor = new GzipCompressor()) {
      byte[] input = "Hello World".getBytes();
      ByteBuffer compressed = compressor.compress(ByteBuffer.wrap(input), 0);
      ByteBuffer decompressed = compressor.decompress(compressed);
      assertEquals(decompressed.array().length, input.length);
      assertEquals(decompressed.array(), input);
      assertEquals("Hello World", new String(decompressed.array()));
    }
  }

  // non-zero start position
  @Test
  public void testCompressWhenByteBufferIsBackedByArrayWithNonZeroStartPosition() throws IOException {
    try (GzipCompressor compressor = new GzipCompressor()) {
      byte[] input = "Hello World".getBytes();
      ByteBuffer compressed = compressor.compress(ByteBuffer.wrap(input), 3);
      ByteBuffer decompressed = compressor.decompress(compressed);
      assertEquals(decompressed.array().length, input.length);
      assertEquals(decompressed.array(), input);
      assertEquals("Hello World", new String(decompressed.array()));
    }
  }

  @Test
  public void testCompressWhenByteBufferIsNotBackedByArray() throws IOException {
    try (GzipCompressor compressor = new GzipCompressor()) {
      byte[] inputBytes = "Hello World".getBytes();
      ByteBuffer inputBuffer = ByteBuffer.allocateDirect(inputBytes.length);
      inputBuffer.put(inputBytes);
      inputBuffer.flip();
      ByteBuffer compressed = compressor.compress(inputBuffer, 0);
      ByteBuffer decompressed = compressor.decompress(compressed);
      assertEquals(decompressed.array().length, inputBytes.length);
      assertEquals(decompressed.array(), inputBytes);
      assertEquals("Hello World", new String(decompressed.array()));
    }
  }

  @Test
  public void testCompressAndDecompress() throws IOException {
    try (GzipCompressor compressor = new GzipCompressor()) {
      byte[] inputBytes = "Hello World".getBytes();
      LogManager.getLogger().info("DEBUGGING: {} {}", inputBytes, inputBytes.length);
      ByteBuffer bbWithHeader = compressor.compress(ByteBuffer.wrap(inputBytes), VeniceCompressor.SCHEMA_HEADER_LENGTH);
      LogManager.getLogger()
          .info("DEBUGGING: {} {} {}", bbWithHeader.position(), bbWithHeader.remaining(), bbWithHeader.array().length);
      bbWithHeader.position(0);
      bbWithHeader.putInt(123);
      LogManager.getLogger()
          .info("DEBUGGING: {} {} {}", bbWithHeader.position(), bbWithHeader.remaining(), bbWithHeader.array().length);
      ByteBuffer decompressed = compressor
          .decompressAndPrependSchemaHeader(bbWithHeader.array(), bbWithHeader.remaining() + bbWithHeader.position());
      LogManager.getLogger()
          .info("DEBUGGING: {} {} {}", decompressed.position(), decompressed.remaining(), decompressed.array().length);
      byte[] outputBytes = new byte[decompressed.remaining()];
      decompressed.get(outputBytes, 0, decompressed.remaining());
      Assert.assertEquals("Hello World", new String(outputBytes));
    }
  }

}
