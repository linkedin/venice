package com.linkedin.venice.compression;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
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

}
