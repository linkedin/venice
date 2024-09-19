package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;

import com.linkedin.davinci.blobtransfer.server.P2PFileTransferServerHandler;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleStateEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestP2PFileTransferServerHandler {
  EmbeddedChannel ch;
  Path baseDir;

  @BeforeMethod
  public void setUp() throws IOException {
    baseDir = Files.createTempDirectory("tmp");
    ch = new EmbeddedChannel(new P2PFileTransferServerHandler(baseDir.toString()));
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
  public void testRejectNonGETMethod() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/test");
    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 405);
  }

  @Test
  public void testRejectInvalidPath() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");
    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 400);
  }

  @Test
  public void testRejectNonExistPath() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10");
    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 404);
  }

  @Test
  public void testFailOnAccessPath() throws IOException {
    // create an empty snapshot dir
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10");

    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 500);
  }

  @Test
  public void testIdleChannelClose() {
    IdleStateEvent event = IdleStateEvent.ALL_IDLE_STATE_EVENT;
    Assert.assertTrue(ch.isOpen());
    ch.pipeline().fireUserEventTriggered(event);
    Assert.assertFalse(ch.isOpen());
  }

  @Test
  public void testTransferSingleFile() throws IOException {
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    Path file1 = snapshotDir.resolve("file1");
    Files.write(file1.toAbsolutePath(), "hello".getBytes());
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10");

    ch.writeInbound(request);
    // start of file1
    Object response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse httpResponse = (DefaultHttpResponse) response;
    Assert.assertEquals(
        httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION),
        "attachment; filename=\"file1\"");
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultFileRegion);
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof LastHttpContent);
    // end of file1
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse endOfTransfer = (DefaultHttpResponse) response;
    Assert.assertEquals(endOfTransfer.headers().get(BLOB_TRANSFER_STATUS), BLOB_TRANSFER_COMPLETED);
    // end of all file
  }

  @Test
  public void testTransferMultipleFiles() throws IOException {
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    Path file1 = snapshotDir.resolve("file1");
    Files.write(file1.toAbsolutePath(), "hello".getBytes());
    Path file2 = snapshotDir.resolve("file2");
    Files.write(file2.toAbsolutePath(), "world".getBytes());
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10");
    Set<String> fileNames = new HashSet<>();
    // the order of file transfer is not guaranteed so put them into a set and remove them one by one
    Collections.addAll(fileNames, "attachment; filename=\"file1\"", "attachment; filename=\"file2\"");
    ch.writeInbound(request);
    // start of file1
    Object response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse httpResponse = (DefaultHttpResponse) response;
    Assert.assertTrue(fileNames.contains(httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION)));
    fileNames.remove(httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION));
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultFileRegion);
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof LastHttpContent);
    // end of file1
    // start of file2
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    httpResponse = (DefaultHttpResponse) response;
    Assert.assertTrue(fileNames.contains(httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION)));
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultFileRegion);
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof LastHttpContent);
    // end of a file2
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse endOfTransfer = (DefaultHttpResponse) response;
    Assert.assertEquals(endOfTransfer.headers().get(BLOB_TRANSFER_STATUS), BLOB_TRANSFER_COMPLETED);
    // end of all file
  }
}
