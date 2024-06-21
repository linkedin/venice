package com.linkedin.venice.blobtransfer.client;

import static com.linkedin.venice.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.venice.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;

import com.linkedin.venice.blobtransfer.BlobTransferPayload;
import com.linkedin.venice.exceptions.VeniceException;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestP2PFileTransferClientHandler {
  EmbeddedChannel ch;
  Path baseDir;
  String TEST_STORE = "test_store";
  int TEST_VERSION = 1;
  int TEST_PARTITION = 0;
  CompletionStage<InputStream> inputStreamFuture;

  @BeforeMethod
  public void setUp() throws IOException {
    baseDir = Files.createTempDirectory("tmp");
    inputStreamFuture = new CompletableFuture<>();
    ch = new EmbeddedChannel(
        new P2PFileTransferClientHandler(
            baseDir.toString(),
            inputStreamFuture,
            TEST_STORE,
            TEST_VERSION,
            TEST_PARTITION));
  }

  @AfterMethod
  public void teardown() throws IOException {
    ch.close();
    Files.walk(baseDir).sorted(Comparator.reverseOrder()).forEach(path -> {
      try {
        Files.delete(path);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  @Test
  public void testFailToGetResponse() {
    DefaultHttpResponse response =
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    ch.writeInbound(response);
    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertEquals(
          e.getCause().getMessage(),
          "Failed to fetch file from remote peer. Response: 500 Internal Server Error");
    }
  }

  @Test
  public void testInvalidResponseHeader() {
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    ch.writeInbound(response);
    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertEquals(e.getCause().getMessage(), "No file name specified in the response for test_store_v1-0");
    }
  }

  @Test
  public void testFileSizeMismatchTransfer() {
    // response
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    // content 1
    // length 1
    HttpContent chunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer("0", CharsetUtil.UTF_8));

    ch.writeInbound(response);
    ch.writeInbound(chunk1);
    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertEquals(e.getCause().getMessage(), "File size mismatch for test_file.txt. Expected: 5, Actual: 1");
    }
  }

  // Technically, it shouldn't happen as the response and content are supposed to arrive in order but just in case
  @Test
  public void testOutOfOrderResponseTransfer() {
    HttpContent chunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer("0", CharsetUtil.UTF_8));

    ch.writeInbound(chunk1);
    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertEquals(e.getCause().getMessage(), "No file opened to write for test_store_v1-0");
    }
  }

  @Test
  public void testSingleFileTransfer() throws ExecutionException, InterruptedException, IOException, TimeoutException {
    // response
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    // content 1
    HttpContent chunk = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));

    // End of a file transfer
    HttpContent endOfFile = LastHttpContent.EMPTY_LAST_CONTENT;

    // End of all file transfer
    DefaultHttpResponse endOfTransfer = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);

    ch.writeInbound(response);
    ch.writeInbound(chunk);
    ch.writeInbound(endOfFile);
    ch.writeInbound(endOfTransfer);
    inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);

    // verify the content is written to the disk
    BlobTransferPayload payload = new BlobTransferPayload(baseDir.toString(), TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Path dest = Paths.get(payload.getPartitionDir());
    Assert.assertTrue(Files.exists(dest));
    Assert.assertTrue(Files.isDirectory(dest));
    Path file1 = dest.resolve("test_file.txt");
    Assert.assertTrue(Files.exists(file1));
    Assert.assertEquals(Files.size(file1), 5);
  }

  @Test
  public void testMultipleFilesTransfer()
      throws ExecutionException, InterruptedException, IOException, TimeoutException {
    // response 1
    DefaultHttpResponse response1 = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response1.headers().add("Content-Disposition", "filename=\"test_file1.txt\"");
    response1.headers().add("Content-Length", "5");
    // response 2
    DefaultHttpResponse response2 = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response2.headers().add("Content-Disposition", "filename=\"test_file2.txt\"");
    response2.headers().add("Content-Length", "10");
    // content
    HttpContent chunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));
    HttpContent chunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("67890", CharsetUtil.UTF_8));
    HttpContent chunk3 = new DefaultLastHttpContent(Unpooled.copiedBuffer("13579", CharsetUtil.UTF_8));

    // End of all file transfer
    DefaultHttpResponse endOfTransfer = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);

    ch.writeInbound(response1);
    ch.writeInbound(chunk1);
    ch.writeInbound(response2);
    ch.writeInbound(chunk2);
    ch.writeInbound(chunk3);
    ch.writeInbound(endOfTransfer);
    inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);

    // verify the content is written to the disk
    BlobTransferPayload payload = new BlobTransferPayload(baseDir.toString(), TEST_STORE, TEST_VERSION, TEST_PARTITION);
    Path dest = Paths.get(payload.getPartitionDir());
    Assert.assertTrue(Files.exists(dest));
    Assert.assertTrue(Files.isDirectory(dest));
    Path file1 = dest.resolve("test_file1.txt");
    Assert.assertTrue(Files.exists(file1));
    Assert.assertEquals(Files.size(file1), 5);
    Path file2 = dest.resolve("test_file2.txt");
    Assert.assertTrue(Files.exists(file2));
    Assert.assertEquals(Files.size(file2), 10);
  }
}
