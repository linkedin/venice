package com.linkedin.venice.grpc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.linkedin.venice.acl.handler.AccessResult;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GrpcUtilsTest {
  private static SSLFactory sslFactory;

  @BeforeTest
  public static void setup() {
    sslFactory = SslUtils.getVeniceLocalSslFactory();
  }

  @Test
  public void testGetTrustManagers() throws Exception {
    TrustManager[] trustManagers = GrpcUtils.getTrustManagers(sslFactory);

    assertNotNull(trustManagers);
    assertTrue(trustManagers.length > 0);
  }

  @Test
  public void testGetKeyManagers() throws Exception {
    KeyManager[] keyManagers = GrpcUtils.getKeyManagers(sslFactory);

    assertNotNull(keyManagers);
    assertTrue(keyManagers.length > 0);
  }

  @Test
  public void testHttpResponseStatusToGrpcStatus() {
    Status grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.GRANTED);
    assertEquals(
        grpcStatus.getCode(),
        Status.OK.getCode(),
        "Mismatch in GRPC status for the http response status permission denied");
    assertEquals(
        AccessResult.GRANTED.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.FORBIDDEN);
    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status permission denied");
    assertEquals(
        AccessResult.FORBIDDEN.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");

    grpcStatus = GrpcUtils.accessResultToGrpcStatus(AccessResult.UNAUTHORIZED);
    assertEquals(
        grpcStatus.getCode(),
        Status.PERMISSION_DENIED.getCode(),
        "Mismatch in GRPC status for the http response status unauthorized");
    assertEquals(
        AccessResult.UNAUTHORIZED.getMessage(),
        grpcStatus.getDescription(),
        "Mismatch in error description for the mapped grpc status");
  }

  @Test
  public void testToByteStringWithArrayBacking() {
    byte[] byteArray = new byte[] { 1, 2, 3, 4, 5 };
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteArray);
    assertTrue(byteBuf.hasArray());

    ByteString byteString = GrpcUtils.toByteString(byteBuf);
    assertEquals(byteString.toByteArray(), byteArray);
  }

  @Test
  public void testToByteStringWithArrayBackingAndReaderIndex() {
    byte[] byteArray = new byte[] { 1, 2, 3, 4, 5 };
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteArray);
    byteBuf.readerIndex(2); // Set reader index to 2

    assertTrue(byteBuf.hasArray());
    ByteString byteString = GrpcUtils.toByteString(byteBuf);

    byte[] expectedArray = new byte[] { 3, 4, 5 }; // Expected array after reader index
    assertEquals(byteString.toByteArray(), expectedArray);
  }

  @Test
  public void testToByteStringWithEmptyByteBuf() {
    ByteBuf byteBuf = Unpooled.EMPTY_BUFFER;

    ByteString byteString = GrpcUtils.toByteString(byteBuf);

    assertTrue(byteString.isEmpty());
  }

  @Test
  public void testToByteStringWithNonArrayBacking() {
    // Create a direct ByteBuf with an initial capacity of 16 bytes
    ByteBuf byteBuf = Unpooled.directBuffer(16);
    // Write some data into the ByteBuf
    byteBuf.writeBytes(new byte[] { 1, 2, 3, 4, 5 });
    assertFalse(byteBuf.hasArray());

    ByteString byteString = GrpcUtils.toByteString(byteBuf);

    byte[] expectedArray = new byte[] { 1, 2, 3, 4, 5 };
    assertEquals(byteString.toByteArray(), expectedArray);
  }

  @Test
  public void testToByteStringWithDifferentOffsetsAndLengths() {
    byte[] byteArray = new byte[] { 10, 20, 30, 40, 50 };
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteArray);
    byteBuf.readerIndex(1); // Start from index 1
    byteBuf.writerIndex(4); // End at index 4

    ByteString byteString = GrpcUtils.toByteString(byteBuf);

    byte[] expectedArray = new byte[] { 20, 30, 40 }; // Expected byte array
    assertEquals(byteString.toByteArray(), expectedArray);
  }

  @Test
  public void testToByteStringWithNioBuffer() {
    byte[] byteArray = new byte[] { 100, (byte) 200, (byte) 255 };
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteArray);

    ByteBuffer nioBuffer = byteBuf.nioBuffer(); // Get NIO buffer
    ByteString byteString = GrpcUtils.toByteString(Unpooled.wrappedBuffer(nioBuffer));

    assertEquals(byteString.toByteArray(), byteArray);
  }

  @Test
  public void testToByteStringWithCompositeByteBuf() {
    // Create some ByteBufs
    ByteBuf buffer1 = Unpooled.wrappedBuffer(new byte[] { 10, 20 });
    ByteBuf buffer2 = Unpooled.wrappedBuffer(new byte[] { 30, 40, 50 });

    // Create a CompositeByteBuf that combines the above ByteBufs
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    compositeByteBuf.addComponents(true, buffer1, buffer2);

    // The expected ByteString combining all bytes from buffer1 and buffer2
    byte[] expectedBytes = new byte[] { 10, 20, 30, 40, 50 };

    // Verify that the CompositeByteBuf has the combined array
    byte[] combinedArray = new byte[5];
    compositeByteBuf.getBytes(0, combinedArray);
    assertEquals(combinedArray, expectedBytes);

    // Convert CompositeByteBuf to ByteString using the method
    ByteString byteString = GrpcUtils.toByteString(compositeByteBuf);

    assertEquals(byteString.toByteArray(), expectedBytes);
  }
}
