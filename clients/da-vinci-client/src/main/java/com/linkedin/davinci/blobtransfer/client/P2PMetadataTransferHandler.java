package com.linkedin.davinci.blobtransfer.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.blobtransfer.BlobTransferPartitionMetadata;
import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The client-side Netty handler to process responses for P2P metadata transfer.
 */
public class P2PMetadataTransferHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
  private static final Logger LOGGER = LogManager.getLogger(P2PMetadataTransferHandler.class);
  private static final InternalAvroSpecificSerializer<StoreVersionState> storeVersionStateSerializer =
      AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer();
  private static final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
      AvroProtocolDefinition.PARTITION_STATE.getSerializer();

  private final BlobTransferPayload payload;
  private BlobTransferPartitionMetadata metadata;
  private StorageMetadataService storageMetadataService;

  public P2PMetadataTransferHandler(
      StorageMetadataService storageMetadataService,
      String baseDir,
      String storeName,
      int version,
      int partition) {
    this.storageMetadataService = storageMetadataService;
    this.payload = new BlobTransferPayload(baseDir, storeName, version, partition);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
    LOGGER.debug("Received metadata response from remote peer for topic {}", payload.getTopicName());

    processMetadata(msg);

    LOGGER.debug(
        "Successfully processed metadata response from remote peer for topic {} with metadata {}",
        payload.getTopicName(),
        metadata);
  }

  private void processMetadata(FullHttpResponse msg) throws IOException {
    if (!msg.status().equals(HttpResponseStatus.OK)) {
      throw new VeniceException("Failed to fetch metadata from remote peer. Response: " + msg.status());
    }

    ByteBuf content = msg.content();
    byte[] metadataBytes = new byte[content.readableBytes()];
    ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
    content.readBytes(metadataBytes);
    BlobTransferPartitionMetadata transferredMetadata =
        objectMapper.readValue(metadataBytes, BlobTransferPartitionMetadata.class);
    if (transferredMetadata == null) {
      throw new VeniceException("No transferPartitionMetadata received for topic " + payload.getTopicName());
    }

    metadata = transferredMetadata;
    updateStorePartitionMetadata(storageMetadataService, metadata);
  }

  /**
   * Sync the offset record and store version state
   * @param storageMetadataService storage metadata service
   * @param transferredPartitionMetadata transferred partition metadata
   */
  public void updateStorePartitionMetadata(
      StorageMetadataService storageMetadataService,
      BlobTransferPartitionMetadata transferredPartitionMetadata) {
    LOGGER.debug("Start updating store partition metadata for topic {}. ", transferredPartitionMetadata.topicName);
    // update the offset record in storage service
    storageMetadataService.put(
        transferredPartitionMetadata.topicName,
        transferredPartitionMetadata.partitionId,
        new OffsetRecord(transferredPartitionMetadata.offsetRecord.array(), partitionStateSerializer));
    // update the metadata SVS
    updateStorageVersionState(storageMetadataService, transferredPartitionMetadata);
  }

  private void updateStorageVersionState(
      StorageMetadataService storageMetadataService,
      BlobTransferPartitionMetadata transferPartitionMetadata) {
    StoreVersionState storeVersionState = storeVersionStateSerializer
        .deserialize(transferPartitionMetadata.topicName, transferPartitionMetadata.storeVersionState.array());
    storageMetadataService.computeStoreVersionState(transferPartitionMetadata.topicName, previousStoreVersionState -> {
      if (previousStoreVersionState != null) {
        previousStoreVersionState.topicSwitch = storeVersionState.topicSwitch;
        return previousStoreVersionState;
      } else {
        return storeVersionState;
      }
    });
  }

  @VisibleForTesting
  public BlobTransferPartitionMetadata getMetadata() {
    return metadata;
  }
}
