package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_COMPLETED;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_STATUS;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BLOB_TRANSFER_TYPE;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferType;
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.davinci.blobtransfer.client.MetadataAggregator;
import com.linkedin.davinci.blobtransfer.client.P2PFileTransferClientHandler;
import com.linkedin.davinci.blobtransfer.client.P2PMetadataTransferHandler;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggBlobTransferStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.IncrementalPushReplicaStatus;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.Mockito;
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
  StorageMetadataService storageMetadataService;
  ExecutorService checksumValidationExecutorService;
  ExecutorService diskWriteExecutorService;
  AggBlobTransferStats blobTransferStats;

  P2PFileTransferClientHandler clientFileHandler;
  P2PMetadataTransferHandler clientMetadataHandler;
  VeniceNotifier veniceNotifier;

  private void setUpChannel(boolean backpressure) throws IOException {
    baseDir = Files.createTempDirectory("tmp");
    inputStreamFuture = new CompletableFuture<>();
    storageMetadataService = Mockito.mock(StorageMetadataService.class);
    blobTransferStats = Mockito.mock(AggBlobTransferStats.class);
    checksumValidationExecutorService = Executors.newSingleThreadExecutor();
    // When backpressure is enabled, the handler offloads disk writes to this executor. We use a
    // direct (caller-runs) executor so that EmbeddedChannel's deterministic single-threaded write-ins
    // retain their original ordering semantics in tests.
    diskWriteExecutorService = backpressure ? DirectExecutor.INSTANCE : null;

    clientFileHandler = Mockito.spy(
        new P2PFileTransferClientHandler(
            baseDir.toString(),
            inputStreamFuture,
            TEST_STORE,
            TEST_VERSION,
            TEST_PARTITION,
            BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE,
            blobTransferStats,
            checksumValidationExecutorService,
            backpressure,
            diskWriteExecutorService));

    veniceNotifier = Mockito.mock(VeniceNotifier.class);
    clientMetadataHandler = Mockito.spy(
        new P2PMetadataTransferHandler(
            storageMetadataService,
            baseDir.toString(),
            TEST_STORE,
            TEST_VERSION,
            TEST_PARTITION,
            BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE,
            () -> veniceNotifier));
    Mockito.doNothing()
        .when(veniceNotifier)
        .startOfIncrementalPushReceived(Mockito.anyString(), Mockito.anyInt(), Mockito.any(), Mockito.anyString());
    Mockito.doNothing()
        .when(veniceNotifier)
        .endOfIncrementalPushReceived(Mockito.anyString(), Mockito.anyInt(), Mockito.any(), Mockito.anyString());

    Mockito.doNothing().when(clientMetadataHandler).updateStorePartitionMetadata(Mockito.any(), Mockito.any());

    ch = new EmbeddedChannel(new MetadataAggregator(1024 * 1024 * 100), clientFileHandler, clientMetadataHandler);
  }

  @BeforeMethod
  public void setUp() throws IOException {
    // Default setup preserves the original (pre-backpressure) behavior. Tests that need to run under
    // both modes explicitly call setUpChannel(boolean) or declare a DataProvider-driven test.
    setUpChannel(false);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    ch.close();
    if (checksumValidationExecutorService != null) {
      checksumValidationExecutorService.shutdownNow();
    }
    Files.walk(baseDir).sorted(Comparator.reverseOrder()).forEach(path -> {
      try {
        Files.delete(path);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  /**
   * Caller-runs executor used in tests so that offloaded disk writes execute synchronously on
   * EmbeddedChannel's calling thread, preserving the test's deterministic message ordering.
   */
  private static final class DirectExecutor extends AbstractExecutorService {
    private static final DirectExecutor INSTANCE = new DirectExecutor();

    @Override
    public void execute(Runnable command) {
      command.run();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
      return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }
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
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
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
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
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

  @Test
  public void testFileChecksumMismatchTransfer() {
    // response
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    // Intentionally not set the checksum

    // content 1
    // length 1
    HttpContent chunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));

    ch.writeInbound(response);
    ch.writeInbound(chunk1);

    // Write BLOB_TRANSFER_COMPLETED as exception is validated there.
    DefaultHttpResponse endOfTransfer = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);
    ch.writeInbound(endOfTransfer);
    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertTrue(e.getCause().getMessage().contains("File checksum mismatch"));
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
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response.headers().add("Content-MD5", checksumGenerateHelper("12345"));

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
    BlobTransferPayload payload = new BlobTransferPayload(
        baseDir.toString(),
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Path dest = Paths.get(payload.getPartitionDir());
    Assert.assertTrue(Files.exists(dest));
    Assert.assertTrue(Files.isDirectory(dest));
    Path file1 = dest.resolve("test_file.txt");
    Assert.assertTrue(Files.exists(file1));
    Assert.assertEquals(Files.size(file1), 5);

    // Verify the temp directory is cleaned up
    Path tempDir = Paths.get(payload.getTempPartitionDir());
    Assert.assertFalse(Files.exists(tempDir), "Temporary directory should be cleaned up after transfer.");
  }

  @Test
  public void testMultipleFilesTransfer()
      throws ExecutionException, InterruptedException, IOException, TimeoutException {
    // response 1
    DefaultHttpResponse response1 = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response1.headers().add("Content-Disposition", "filename=\"test_file1.txt\"");
    response1.headers().add("Content-Length", "5");
    response1.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response1.headers().add("Content-MD5", checksumGenerateHelper("12345"));
    // response 2
    DefaultHttpResponse response2 = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response2.headers().add("Content-Disposition", "filename=\"test_file2.txt\"");
    response2.headers().add("Content-Length", "10");
    response2.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response2.headers().add("Content-MD5", checksumGenerateHelper("6789013579"));

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
    BlobTransferPayload payload = new BlobTransferPayload(
        baseDir.toString(),
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE);
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

  @Test
  public void testSingleMetadataTransfer()
      throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
    BlobTransferPartitionMetadata expectedMetadata = new BlobTransferPartitionMetadata();
    expectedMetadata.setTopicName(TEST_STORE + "_v" + TEST_VERSION);
    expectedMetadata.setPartitionId(TEST_PARTITION);
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    Map<String, IncrementalPushReplicaStatus> incrementalPushInfo = new HashMap<>();
    incrementalPushInfo.put(
        "pushJobVersion1",
        new IncrementalPushReplicaStatus(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 1000L));
    incrementalPushInfo.put(
        "pushJobVersion2",
        new IncrementalPushReplicaStatus(ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED.getValue(), 2000L));
    offsetRecord.setTrackingIncrementalPushStatus(incrementalPushInfo);

    expectedMetadata.setOffsetRecord(ByteBuffer.wrap(offsetRecord.toBytes()));

    ObjectMapper objectMapper = new ObjectMapper();
    String metadataJson = objectMapper.writeValueAsString(expectedMetadata);
    byte[] metadataBytes = metadataJson.getBytes(CharsetUtil.UTF_8);

    FullHttpResponse metadataResponse = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        Unpooled.copiedBuffer(metadataJson, CharsetUtil.UTF_8));
    metadataResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, metadataBytes.length);
    metadataResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    metadataResponse.headers().set(BLOB_TRANSFER_TYPE, BlobTransferType.METADATA);

    DefaultHttpResponse endOfTransfer = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);

    // Simulate inbound data for the metadata response
    ch.writeInbound(metadataResponse);
    ch.writeInbound(endOfTransfer);

    // Ensure the future is completed
    inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);

    // Verify that the metadata was correctly parsed and handled
    BlobTransferPartitionMetadata actualMetadata = clientMetadataHandler.getMetadata();
    Assert.assertNotNull(actualMetadata);
    Assert.assertEquals(actualMetadata.getTopicName(), expectedMetadata.getTopicName());
    Assert.assertEquals(actualMetadata.getPartitionId(), expectedMetadata.getPartitionId());
    Assert.assertEquals(actualMetadata.getOffsetRecord(), expectedMetadata.getOffsetRecord());
    Assert.assertTrue(inputStreamFuture.toCompletableFuture().isDone());
  }

  @Test
  public void testMultipleFilesAndOneMetadataTransfer()
      throws ExecutionException, InterruptedException, IOException, TimeoutException {
    // Transfer of two files
    // File 1 response
    DefaultHttpResponse response1 = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response1.headers().add("Content-Disposition", "filename=\"test_file1.txt\"");
    response1.headers().add("Content-Length", "5");
    response1.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response1.headers().add("Content-MD5", checksumGenerateHelper("12345"));

    // File 2 response
    DefaultHttpResponse response2 = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response2.headers().add("Content-Disposition", "filename=\"test_file2.txt\"");
    response2.headers().add("Content-Length", "10");
    response2.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response2.headers().add("Content-MD5", checksumGenerateHelper("6789013579"));

    // File content chunks
    HttpContent chunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));
    HttpContent chunk2 = new DefaultHttpContent(Unpooled.copiedBuffer("67890", CharsetUtil.UTF_8));
    HttpContent chunk3 = new DefaultLastHttpContent(Unpooled.copiedBuffer("13579", CharsetUtil.UTF_8));

    // Simulate inbound data for file transfer
    ch.writeInbound(response1);
    ch.writeInbound(chunk1);
    ch.writeInbound(response2);
    ch.writeInbound(chunk2);
    ch.writeInbound(chunk3);

    // Transfer metadata
    // Prepare metadata
    BlobTransferPartitionMetadata expectMetadata = new BlobTransferPartitionMetadata();
    expectMetadata.setTopicName(TEST_STORE + "_v" + TEST_VERSION);
    expectMetadata.setPartitionId(TEST_PARTITION);
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    expectMetadata.setOffsetRecord(ByteBuffer.wrap(offsetRecord.toBytes()));

    // Serialize metadata to JSON
    ObjectMapper objectMapper = new ObjectMapper();
    String metadataJson = objectMapper.writeValueAsString(expectMetadata);
    byte[] metadataBytes = metadataJson.getBytes(CharsetUtil.UTF_8);
    // Metadata HTTP response
    FullHttpResponse metadataResponse = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.OK,
        Unpooled.copiedBuffer(metadataJson, CharsetUtil.UTF_8));
    metadataResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, metadataBytes.length);
    metadataResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
    metadataResponse.headers().set(BLOB_TRANSFER_TYPE, BlobTransferType.METADATA);

    // Simulate inbound data for the metadata transfer
    ch.writeInbound(metadataResponse);

    // End of metadata transfer
    DefaultHttpResponse endOfMetadataTransfer =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfMetadataTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);
    ch.writeInbound(endOfMetadataTransfer);

    // Ensure the future is completed
    inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);

    // Verify the files are written to disk
    BlobTransferPayload payload = new BlobTransferPayload(
        baseDir.toString(),
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Path dest = Paths.get(payload.getPartitionDir());
    Assert.assertTrue(Files.exists(dest));
    Assert.assertTrue(Files.isDirectory(dest));

    // Verify file 1
    Path file1 = dest.resolve("test_file1.txt");
    Assert.assertTrue(Files.exists(file1));
    Assert.assertEquals(Files.size(file1), 5);

    // Verify file 2
    Path file2 = dest.resolve("test_file2.txt");
    Assert.assertTrue(Files.exists(file2));
    Assert.assertEquals(Files.size(file2), 10);

    // Verify the metadata was correctly parsed and handled
    BlobTransferPartitionMetadata actualMetadata = clientMetadataHandler.getMetadata();
    Assert.assertNotNull(actualMetadata);
    Assert.assertEquals(actualMetadata.getTopicName(), expectMetadata.getTopicName());
    Assert.assertEquals(actualMetadata.getPartitionId(), expectMetadata.getPartitionId());
    Assert.assertEquals(actualMetadata.getOffsetRecord(), expectMetadata.getOffsetRecord());

    // Verify the temp directory is cleaned up
    Path tempDir = Paths.get(payload.getTempPartitionDir());
    Assert.assertFalse(Files.exists(tempDir), "Temporary directory should be cleaned up after transfer.");

    // Ensure the future is completed
    Assert.assertTrue(inputStreamFuture.toCompletableFuture().isDone());
  }

  @Test
  public void testChannelInactiveBeforeTransferComplete() throws IOException {
    // Prepare, response is not complete yet.
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response.headers().add("Content-MD5", checksumGenerateHelper("12345"));

    ch.writeInbound(response);
    // Simulate server graceful shutdown by closing channel
    ch.close();

    // Verification
    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.SECONDS);
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertTrue(e.getCause().getMessage().contains("might due to server graceful shutdown"));
      Assert.assertTrue(e.getCause().getMessage().contains("test_store_v1-0"));
    }
  }

  /**
   * Regression test for the finalizing-state race introduced when {@code handleEndOfTransfer} was refactored to
   * complete asynchronously. Once the end-of-transfer marker has been processed, the transfer future stays
   * incomplete for a short window while checksum validation runs on a separate executor. A peer-side FIN
   * (channelInactive) during that window must NOT be treated as an incomplete transfer — all bytes are already
   * on disk and the async path owns finalization. Before the fix, this would spuriously fail the transfer with
   * "might due to server graceful shutdown or timeout".
   */
  @Test
  public void testChannelInactiveAfterEndOfTransferDoesNotFastFailover() throws Exception {
    // Complete a single-file transfer so handleEndOfTransfer is entered. With the default (non-backpressure)
    // setup, checksum validation runs on a real SingleThreadExecutor; by the time we reach the assertions below
    // it has completed and the future is done. The key behaviour under test is that ch.close() AFTER EndOfTransfer
    // does not race with finalization to fail the future exceptionally.
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response.headers().add("Content-MD5", checksumGenerateHelper("12345"));

    HttpContent chunk = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));
    DefaultHttpResponse endOfTransfer = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);

    ch.writeInbound(response);
    ch.writeInbound(chunk);
    ch.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeInbound(endOfTransfer);

    // Simulate peer-side FIN AFTER the end-of-transfer marker was observed. This must not flip the future into
    // an exceptional completion.
    ch.close();

    inputStreamFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
    Assert.assertTrue(inputStreamFuture.toCompletableFuture().isDone());
    Assert.assertFalse(
        inputStreamFuture.toCompletableFuture().isCompletedExceptionally(),
        "inputStreamFuture must not be completed exceptionally when ch.close() lands after EndOfTransfer");
  }

  /**
   * Mirror of {@link #testChannelInactiveAfterEndOfTransferDoesNotFastFailover} for the READER_IDLE path.
   */
  @Test
  public void testReaderIdleAfterEndOfTransferDoesNotFastFailover() throws Exception {
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response.headers().add("Content-MD5", checksumGenerateHelper("12345"));

    HttpContent chunk = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));
    DefaultHttpResponse endOfTransfer = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);

    ch.writeInbound(response);
    ch.writeInbound(chunk);
    ch.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeInbound(endOfTransfer);

    // Fire a READER_IDLE AFTER EndOfTransfer has been observed; the handler should treat it as "no more data is
    // expected, we've already received everything" and NOT fast-failover.
    ch.pipeline().fireUserEventTriggered(IdleStateEvent.READER_IDLE_STATE_EVENT);

    inputStreamFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
    Assert.assertTrue(inputStreamFuture.toCompletableFuture().isDone());
    Assert.assertFalse(
        inputStreamFuture.toCompletableFuture().isCompletedExceptionally(),
        "inputStreamFuture must not be completed exceptionally when READER_IDLE lands after EndOfTransfer");
  }

  /**
   * Regression test for the {@code exceptionCaught} finalizing-state guard. A pipeline-level exception (TCP RST,
   * SSL decode error, etc.) landing during the async checksum/rename window must NOT flip the transfer future
   * into an exceptional completion — all bytes are already on disk by then, and {@code cleanupResources} would
   * otherwise delete the fully-received temp partition directory, causing the outer {@code NettyFileTransferClient}
   * failover loop to re-fetch the same replica from another peer unnecessarily.
   *
   * <p>Before the guard was added, this scenario would destroy successfully-transferred partition data.
   */
  @Test
  public void testExceptionCaughtAfterEndOfTransferDoesNotFastFailover() throws Exception {
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response.headers().add("Content-MD5", checksumGenerateHelper("12345"));

    HttpContent chunk = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));
    DefaultHttpResponse endOfTransfer = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);

    ch.writeInbound(response);
    ch.writeInbound(chunk);
    ch.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeInbound(endOfTransfer);

    // Fire a pipeline exception AFTER EndOfTransfer has been observed. Simulates TCP RST / SSL decode error that
    // can arrive on a stale stray packet while the async checksum/rename chain is still finalizing.
    ch.pipeline().fireExceptionCaught(new RuntimeException("simulated TCP RST during checksum window"));

    inputStreamFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
    Assert.assertTrue(inputStreamFuture.toCompletableFuture().isDone());
    Assert.assertFalse(
        inputStreamFuture.toCompletableFuture().isCompletedExceptionally(),
        "inputStreamFuture must not be completed exceptionally when a pipeline exception lands during the "
            + "post-EndOfTransfer finalization window");

    // Confirm the transferred file survived (i.e. cleanupResources did NOT run).
    BlobTransferPayload payload = new BlobTransferPayload(
        baseDir.toString(),
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Path dest = Paths.get(payload.getPartitionDir()).resolve("test_file.txt");
    Assert.assertTrue(Files.exists(dest), "Transferred file must not be deleted by a post-EOT pipeline exception");
    Assert.assertEquals(Files.size(dest), 5);
  }

  @Test
  public void testReaderIdleEventBeforeTransferComplete() throws IOException {
    // Prepare a non complete response
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response.headers().add("Content-MD5", checksumGenerateHelper("12345"));

    ch.writeInbound(response);
    // Simulate READER_IDLE timeout event
    IdleStateEvent idleEvent = IdleStateEvent.READER_IDLE_STATE_EVENT;
    ch.pipeline().fireUserEventTriggered(idleEvent);

    // Assert: Future should complete exceptionally
    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.SECONDS);
      Assert.fail("Expected exception not thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertTrue(
          e.getCause()
              .getMessage()
              .contains("Channel idle before completing transfer, might due to server unexpected abrupt termination."));
      Assert.assertTrue(e.getCause().getMessage().contains("test_store_v1-0"));
      Assert.assertTrue(e.getCause().getMessage().contains("channel active:"));
    }
  }

  /**
   * Generate checksum via string content
   */
  private String checksumGenerateHelper(String content) throws IOException {
    Path tempFile = baseDir.resolve("temporaryFile.txt");
    Files.write(tempFile.toAbsolutePath(), content.getBytes());
    String checksum = BlobTransferUtils.generateFileChecksum(tempFile);
    Files.delete(tempFile);
    return checksum;
  }

  /**
   * Runs the core single-file transfer flow under both backpressure modes to ensure the backpressure path
   * produces the same end-state as the legacy path.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSingleFileTransferUnderBothBackpressureModes(boolean enableBackpressure) throws IOException {
    tearDown(); // discard default-mode setup from @BeforeMethod
    setUpChannel(enableBackpressure);

    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    response.headers().add("Content-MD5", checksumGenerateHelper("12345"));

    HttpContent chunk = new DefaultLastHttpContent(Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8));
    HttpContent endOfFile = LastHttpContent.EMPTY_LAST_CONTENT;
    DefaultHttpResponse endOfTransfer = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    endOfTransfer.headers().add(BLOB_TRANSFER_STATUS, BLOB_TRANSFER_COMPLETED);

    ch.writeInbound(response);
    ch.writeInbound(chunk);
    ch.writeInbound(endOfFile);
    ch.writeInbound(endOfTransfer);

    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Single-file transfer should succeed under backpressure=" + enableBackpressure, e);
    }

    BlobTransferPayload payload = new BlobTransferPayload(
        baseDir.toString(),
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE);
    Path dest = Paths.get(payload.getPartitionDir());
    Assert.assertTrue(Files.exists(dest), "Partition dir should exist for backpressure=" + enableBackpressure);
    Path file1 = dest.resolve("test_file.txt");
    Assert.assertTrue(Files.exists(file1), "Transferred file should exist for backpressure=" + enableBackpressure);
    Assert.assertEquals(Files.size(file1), 5, "File size must match payload for backpressure=" + enableBackpressure);
  }

  /**
   * Runs the size-mismatch failure path under both backpressure modes to ensure errors thrown during the
   * content-processing branch surface through exceptionCaught regardless of whether the write ran on the
   * event loop or on the disk-write executor.
   */
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testFileSizeMismatchUnderBothBackpressureModes(boolean enableBackpressure) throws IOException {
    tearDown();
    setUpChannel(enableBackpressure);

    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "5");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    HttpContent chunk1 = new DefaultLastHttpContent(Unpooled.copiedBuffer("0", CharsetUtil.UTF_8));

    ch.writeInbound(response);
    ch.writeInbound(chunk1);

    try {
      inputStreamFuture.toCompletableFuture().get(1, TimeUnit.MINUTES);
      Assert.fail("Size mismatch should propagate under backpressure=" + enableBackpressure);
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceException);
      Assert.assertTrue(
          e.getCause().getMessage().contains("File size mismatch for test_file.txt"),
          "Unexpected message under backpressure=" + enableBackpressure + ": " + e.getCause().getMessage());
    }
  }

  /**
   * In backpressure mode the handler must drive socket reads manually by calling ctx.read() in channelActive
   * (since AUTO_READ is disabled on the bootstrap). This test verifies the handler requests an initial read
   * exactly once on channel activation when backpressure is enabled, and does NOT request one when disabled.
   */
  @Test
  public void testChannelActiveTriggersReadOnlyWhenBackpressureEnabled() throws Exception {
    // backpressure OFF — handler should rely on AUTO_READ, no explicit read()
    tearDown();
    setUpChannel(false);
    io.netty.channel.ChannelHandlerContext ctx = Mockito.mock(io.netty.channel.ChannelHandlerContext.class);
    clientFileHandler.channelActive(ctx);
    Mockito.verify(ctx, Mockito.never()).read();

    // backpressure ON — handler must prime the read pump
    tearDown();
    setUpChannel(true);
    ctx = Mockito.mock(io.netty.channel.ChannelHandlerContext.class);
    clientFileHandler.channelActive(ctx);
    Mockito.verify(ctx, Mockito.times(1)).read();
  }

  /**
   * Regression test for the drain/release ordering in {@link
   * com.linkedin.davinci.blobtransfer.client.P2PFileTransferClientHandler#handleExceptionGracefully}: when the
   * channel closes while HttpContent chunks are still queued for the disk executor, every retained ByteBuf must
   * reach refCnt=0 (otherwise direct memory leaks under the exact OOM scenario this feature was built to fix).
   *
   * Implementation detail: we substitute a never-executing executor for the disk pool so queued runnables are NOT
   * auto-drained by a prior disk task completion. The channelInactive → handleExceptionGracefully path is the sole
   * thing that must release the retained buffer.
   */
  @Test
  public void testPendingContentReleasedOnChannelCloseUnderBackpressure() throws IOException {
    tearDown();
    // Re-initialize with backpressure on, but install a disk executor that drops every submission silently so the
    // queued content stays enqueued until channel close forces cleanup.
    this.baseDir = Files.createTempDirectory("tmp");
    this.inputStreamFuture = new CompletableFuture<>();
    this.storageMetadataService = Mockito.mock(StorageMetadataService.class);
    this.blobTransferStats = Mockito.mock(AggBlobTransferStats.class);
    this.checksumValidationExecutorService = Executors.newSingleThreadExecutor();
    this.diskWriteExecutorService = new AbstractExecutorService() {
      @Override
      public void execute(Runnable command) {
        // Deliberately do nothing: we want the task to stay pending so channel close has to drain it.
      }

      @Override
      public void shutdown() {
      }

      @Override
      public List<Runnable> shutdownNow() {
        return Collections.emptyList();
      }

      @Override
      public boolean isShutdown() {
        return false;
      }

      @Override
      public boolean isTerminated() {
        return false;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
      }
    };

    this.clientFileHandler = new P2PFileTransferClientHandler(
        baseDir.toString(),
        inputStreamFuture,
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE,
        blobTransferStats,
        checksumValidationExecutorService,
        true,
        diskWriteExecutorService);
    this.veniceNotifier = Mockito.mock(VeniceNotifier.class);
    this.clientMetadataHandler = new P2PMetadataTransferHandler(
        storageMetadataService,
        baseDir.toString(),
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE,
        () -> veniceNotifier);
    this.ch = new EmbeddedChannel(new MetadataAggregator(1024 * 1024 * 100), clientFileHandler, clientMetadataHandler);

    // Set up enough state to pass the HttpResponse header check, then enqueue TWO HttpContent chunks. The first is
    // polled off the queue and submitted to the no-op executor (so it "leaks" — that's a limitation of the fake
    // executor, not the production code path). The second chunk stays in pendingDiskTasks because the single-flight
    // flag is already set, giving releasePendingContents actual work to drain on channel close.
    DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    response.headers().add("Content-Disposition", "filename=\"test_file.txt\"");
    response.headers().add("Content-Length", "10");
    response.headers().add(BLOB_TRANSFER_TYPE, BlobTransferType.FILE);
    ch.writeInbound(response);

    io.netty.buffer.ByteBuf firstPayload = Unpooled.copiedBuffer("12345", CharsetUtil.UTF_8);
    io.netty.buffer.ByteBuf secondPayload = Unpooled.copiedBuffer("67890", CharsetUtil.UTF_8);
    try {
      ch.writeInbound(new io.netty.handler.codec.http.DefaultHttpContent(firstPayload));
      ch.writeInbound(new DefaultLastHttpContent(secondPayload));
      // Sanity: the second chunk's buffer should still be live (retained in pendingDiskTasks waiting for the
      // (never-arriving) completion of the first chunk's disk task).
      Assert.assertTrue(
          secondPayload.refCnt() >= 1,
          "Expected second chunk to be retained by the handler while queued behind the first in-flight task");

      // Close the channel — this fires channelInactive → fastFailoverIncompleteTransfer → handleExceptionGracefully
      // → completeExceptionally (so drained tasks short-circuit on isDone()) → releasePendingContents, which must
      // release the still-queued second chunk's retained ByteBuf even though its disk task never ran.
      ch.close();

      Assert.assertEquals(
          secondPayload.refCnt(),
          0,
          "Retained second HttpContent ByteBuf must be released when the channel closes with pending disk tasks");
    } finally {
      // firstPayload's disk task was intentionally dropped by the no-op executor, so the handler never releases it
      // and Netty leak detection would otherwise flag a leaked direct buffer at test-suite shutdown. Release any
      // remaining refs here to keep the test hermetic; this cleanup is specific to the fake executor — the
      // production code path completes every submitted task.
      if (firstPayload.refCnt() > 0) {
        firstPayload.release(firstPayload.refCnt());
      }
      if (secondPayload.refCnt() > 0) {
        secondPayload.release(secondPayload.refCnt());
      }
    }
  }

  /**
   * Constructor guard: backpressure enabled without a disk-write executor should fail fast.
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBackpressureWithoutDiskExecutorRejected() {
    new com.linkedin.davinci.blobtransfer.client.P2PFileTransferClientHandler(
        baseDir.toString(),
        new CompletableFuture<>(),
        TEST_STORE,
        TEST_VERSION,
        TEST_PARTITION,
        BlobTransferUtils.BlobTransferTableFormat.BLOCK_BASED_TABLE,
        blobTransferStats,
        checksumValidationExecutorService,
        true,
        null);
  }
}
