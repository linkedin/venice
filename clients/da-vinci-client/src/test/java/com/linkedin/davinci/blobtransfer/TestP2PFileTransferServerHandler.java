package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_TYPE;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.blobtransfer.server.P2PFileTransferServerHandler;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleStateEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestP2PFileTransferServerHandler {
  EmbeddedChannel ch;
  Path baseDir;
  StorageMetadataService storageMetadataService;
  P2PFileTransferServerHandler serverHandler;

  @BeforeMethod
  public void setUp() throws IOException {
    baseDir = Files.createTempDirectory("tmp");
    storageMetadataService = Mockito.mock(StorageMetadataService.class);
    serverHandler = new P2PFileTransferServerHandler(baseDir.toString(), storageMetadataService);
    ch = new EmbeddedChannel(serverHandler);
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
  public void testTransferSingleFileAndSingleMetadata() throws IOException {
    // prepare response from metadata service
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt());

    // prepare the file request
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
    Assert.assertEquals(httpResponse.headers().get(BLOB_TRANSFER_TYPE), BlobTransferType.FILE.toString());
    // send the content in one chunk
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultFileRegion);
    // the last empty response for file1
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof LastHttpContent);
    // end of file1

    // start of metadata
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse metadataResponse = (DefaultHttpResponse) response;
    Assert.assertEquals(metadataResponse.headers().get(BLOB_TRANSFER_TYPE), BlobTransferType.METADATA.toString());
    // end of metadata

    // start of STATUS response
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse endOfTransfer = (DefaultHttpResponse) response;
    Assert.assertEquals(endOfTransfer.headers().get(BLOB_TRANSFER_STATUS), BLOB_TRANSFER_COMPLETED);
    // end of STATUS response
  }

  @Test
  public void testTransferMultipleFiles() throws IOException {
    // prepare response from metadata service
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt());

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

    // start of metadata
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof FullHttpResponse);
    FullHttpResponse metadataResponse = (FullHttpResponse) response;
    Assert.assertEquals(metadataResponse.headers().get(BLOB_TRANSFER_TYPE), BlobTransferType.METADATA.toString());

    ByteBuf content = metadataResponse.content();
    byte[] metadataBytes = new byte[content.readableBytes()];
    content.readBytes(metadataBytes);
    ObjectMapper objectMapper = new ObjectMapper();
    BlobTransferPartitionMetadata metadata = objectMapper.readValue(metadataBytes, BlobTransferPartitionMetadata.class);

    Assert.assertEquals(metadata.getTopicName(), "myStore_v1");
    Assert.assertEquals(metadata.getPartitionId(), 10);
    Assert.assertEquals(metadata.getOffsetRecord(), ByteBuffer.wrap(offsetRecord.toBytes()));

    InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
    java.nio.ByteBuffer storeVersionStateByte =
        ByteBuffer.wrap(storeVersionStateSerializer.serialize(metadata.getTopicName(), storeVersionState));
    Assert.assertEquals(metadata.getStoreVersionState(), storeVersionStateByte);
    // end of metadata

    // start of STATUS response
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse endOfTransfer = (DefaultHttpResponse) response;
    Assert.assertEquals(endOfTransfer.headers().get(BLOB_TRANSFER_STATUS), BLOB_TRANSFER_COMPLETED);
    // end of STATUS response
  }

  @Test
  public void testWhenMetadataCreateError() throws IOException {
    // prepare the file request
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
    Assert.assertEquals(httpResponse.headers().get(BLOB_TRANSFER_TYPE), BlobTransferType.FILE.toString());
    // send the content in one chunk
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultFileRegion);
    // the last empty response for file1
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof LastHttpContent);
    // end of file1

    // metadata in server side has error
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    Assert.assertEquals(((DefaultHttpResponse) response).status(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
  }
}
