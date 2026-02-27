package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_TYPE;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferType;
import static com.linkedin.venice.response.VeniceReadResponseStatus.TOO_MANY_REQUESTS;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.ArgumentMatchers.any;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.blobtransfer.server.P2PFileTransferServerHandler;
import com.linkedin.davinci.stats.AggBlobTransferStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
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
  int blobTransferMaxTimeoutInMin;
  StorageMetadataService storageMetadataService;
  P2PFileTransferServerHandler serverHandler;
  BlobSnapshotManager blobSnapshotManager;
  StorageEngineRepository storageEngineRepository;
  AggBlobTransferStats blobTransferStats;
  int maxAllowedConcurrentSnapshotUsers = 20;

  @BeforeMethod
  public void setUp() throws IOException {
    baseDir = Files.createTempDirectory("tmp");
    blobTransferMaxTimeoutInMin = 30;
    storageMetadataService = Mockito.mock(StorageMetadataService.class);
    storageEngineRepository = Mockito.mock(StorageEngineRepository.class);
    blobTransferStats = Mockito.mock(AggBlobTransferStats.class);
    blobSnapshotManager = Mockito.spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    serverHandler = new P2PFileTransferServerHandler(
        baseDir.toString(),
        blobTransferMaxTimeoutInMin,
        blobSnapshotManager,
        blobTransferStats,
        maxAllowedConcurrentSnapshotUsers);
    ch = new EmbeddedChannel(serverHandler);
  }

  @AfterMethod
  public void tearDown() throws IOException {
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

  /**
   * Testing the method is GET, but uri format is invalid
   */
  @Test
  public void testRejectInvalidPath() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/test");
    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 500);
  }

  @Test
  public void testRejectTooManyRequest() throws IOException {
    StorageEngine localStorageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(localStorageEngine).when(storageEngineRepository).getLocalStorageEngine(Mockito.any());
    Mockito.doReturn(true).when(localStorageEngine).containsPartition(Mockito.anyInt());

    // prepare response from metadata service
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    // prepare the file request
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    Path file1 = snapshotDir.resolve("file1");
    Files.write(file1.toAbsolutePath(), "hello".getBytes());
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(Mockito.anyString(), Mockito.anyInt());

    // Send maxAllowedConcurrentSnapshotUsers + 1 requests
    for (int requestCount = 0; requestCount < maxAllowedConcurrentSnapshotUsers; requestCount++) {
      FullHttpRequest request =
          new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10/BLOCK_BASED_TABLE");
      ch.writeInbound(request);
    }

    // Read all outbound responses and check if at least one is 429
    boolean foundTooManyRequestsResponse = false;
    while (true) {
      Object outbound = ch.readOutbound();
      if (outbound == null) {
        break;
      }

      if (outbound instanceof FullHttpResponse) {
        FullHttpResponse httpResponse = (FullHttpResponse) outbound;
        if (httpResponse.status().code() == TOO_MANY_REQUESTS.getCode()) {
          foundTooManyRequestsResponse = true;
          break;
        }
      }
    }

    Assert.assertTrue(foundTooManyRequestsResponse);
  }

  @Test
  public void testRejectNonExistPath() {
    // prepare response from metadata service for the metadata preparation
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10/BLOCK_BASED_TABLE");
    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 404);
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers("myStore_v1", 10), 0);
  }

  /**
   * Sending request for plain table format, but the snapshot manager is use the block based format.
   */
  @Test
  public void testRejectNotMatchFormat() throws IOException {
    // prepare the file request
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    Path file1 = snapshotDir.resolve("file1");
    Files.write(file1.toAbsolutePath(), "hello".getBytes());

    // prepare response from metadata service for the metadata preparation
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10/PLAIN_TABLE");
    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 404);
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers("myStore_v1", 10), 0);
  }

  @Test
  public void testFailOnAccessPath() throws IOException {
    StorageEngine localStorageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(localStorageEngine).when(storageEngineRepository).getLocalStorageEngine(Mockito.any());
    Mockito.doReturn(true).when(localStorageEngine).containsPartition(Mockito.anyInt());
    // prepare response from metadata service for the metadata preparation
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    // create an empty snapshot dir
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10/BLOCK_BASED_TABLE");
    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(Mockito.anyString(), Mockito.anyInt());

    ch.writeInbound(request);
    FullHttpResponse response = ch.readOutbound();
    Assert.assertEquals(response.status().code(), 500);
    // make the ch inactive
    ch.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers("myStore_v1", 10), 0);
  }

  @Test
  public void testIdleChannelClose() {
    IdleStateEvent event = IdleStateEvent.ALL_IDLE_STATE_EVENT;
    Assert.assertTrue(ch.isOpen());
    ch.pipeline().fireUserEventTriggered(event);
    Assert.assertFalse(ch.isOpen());
  }

  @Test
  public void testTransferSingleFileAndSingleMetadataForBatchStore() throws IOException {
    StorageEngine localStorageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(localStorageEngine).when(storageEngineRepository).getLocalStorageEngine(Mockito.any());
    Mockito.doReturn(true).when(localStorageEngine).containsPartition(Mockito.anyInt());

    // prepare response from metadata service
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    // prepare the file request
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    Path file1 = snapshotDir.resolve("file1");
    Files.write(file1.toAbsolutePath(), "hello".getBytes());
    String file1ChecksumHeader = BlobTransferUtils.generateFileChecksum(file1);
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10/BLOCK_BASED_TABLE");

    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(Mockito.anyString(), Mockito.anyInt());

    ch.writeInbound(request);

    // start of file1
    Object response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse httpResponse = (DefaultHttpResponse) response;
    Assert.assertEquals(
        httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION),
        "attachment; filename=\"file1\"");
    Assert.assertEquals(httpResponse.headers().get(HttpHeaderNames.CONTENT_MD5), file1ChecksumHeader);
    Assert.assertEquals(httpResponse.headers().get(BLOB_TRANSFER_TYPE), BlobTransferType.FILE.toString());
    // send the content in one chunk
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof HttpChunkedInput);
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

    // make the ch inactive
    ch.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers("myStore_v1", 10), 0);
  }

  @Test
  public void testTransferMultipleFiles() throws IOException {
    StorageEngine localStorageEngine = Mockito.mock(StorageEngine.class);
    Mockito.doReturn(localStorageEngine).when(storageEngineRepository).getLocalStorageEngine(Mockito.any());
    Mockito.doReturn(true).when(localStorageEngine).containsPartition(Mockito.anyInt());

    // prepare response from metadata service
    StoreVersionState storeVersionState = new StoreVersionState();
    Mockito.doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(Mockito.any());

    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    Mockito.doReturn(offsetRecord).when(storageMetadataService).getLastOffset(Mockito.any(), Mockito.anyInt(), any());

    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);

    Path file1 = snapshotDir.resolve("file1");
    Files.write(file1.toAbsolutePath(), "hello".getBytes());
    String file1ChecksumHeader = BlobTransferUtils.generateFileChecksum(file1);

    Path file2 = snapshotDir.resolve("file2");
    Files.write(file2.toAbsolutePath(), "world".getBytes());
    String file2ChecksumHeader = BlobTransferUtils.generateFileChecksum(file2);

    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10/BLOCK_BASED_TABLE");
    Set<String> fileNames = new HashSet<>();
    Set<String> fileChecksums = new HashSet<>();
    // the order of file transfer is not guaranteed so put them into a set and remove them one by one
    Collections.addAll(fileNames, "attachment; filename=\"file1\"", "attachment; filename=\"file2\"");
    Collections.addAll(fileChecksums, file1ChecksumHeader, file2ChecksumHeader);

    Mockito.doNothing().when(blobSnapshotManager).createSnapshot(Mockito.anyString(), Mockito.anyInt());

    ch.writeInbound(request);
    // start of file1
    Object response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    DefaultHttpResponse httpResponse = (DefaultHttpResponse) response;
    Assert.assertTrue(fileNames.contains(httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION)));
    Assert.assertTrue(fileChecksums.contains(httpResponse.headers().get(HttpHeaderNames.CONTENT_MD5)));
    fileNames.remove(httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION));
    fileChecksums.remove(httpResponse.headers().get(HttpHeaderNames.CONTENT_MD5));
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof HttpChunkedInput);
    // end of file1

    // start of file2
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    httpResponse = (DefaultHttpResponse) response;
    Assert.assertTrue(fileNames.contains(httpResponse.headers().get(HttpHeaderNames.CONTENT_DISPOSITION)));
    Assert.assertTrue(fileChecksums.contains(httpResponse.headers().get(HttpHeaderNames.CONTENT_MD5)));
    response = ch.readOutbound();
    Assert.assertTrue(response instanceof HttpChunkedInput);
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

    // make the ch inactive
    ch.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers("myStore_v1", 10), 0);
  }

  /**
   * Test when fail to get the metadata from storageMetadataService, it should return error to client.
   * @throws IOException
   */
  @Test
  public void testWhenMetadataCreateError() throws IOException {
    // prepare the file request
    Path snapshotDir = Paths.get(RocksDBUtils.composeSnapshotDir(baseDir.toString(), "myStore_v1", 10));
    Files.createDirectories(snapshotDir);
    Path file1 = snapshotDir.resolve("file1");
    Files.write(file1.toAbsolutePath(), "hello".getBytes());
    FullHttpRequest request =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/myStore/1/10/BLOCK_BASED_TABLE");

    ch.writeInbound(request);

    // metadata in server side has error
    Object response = ch.readOutbound();
    Assert.assertTrue(response instanceof DefaultHttpResponse);
    Assert.assertEquals(((DefaultHttpResponse) response).status(), HttpResponseStatus.NOT_FOUND);

    Assert.assertEquals(blobSnapshotManager.getConcurrentSnapshotUsers("myStore_v1", 10), 0);
  }
}
