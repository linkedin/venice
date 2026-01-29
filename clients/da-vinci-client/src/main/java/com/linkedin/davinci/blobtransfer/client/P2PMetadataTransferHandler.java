package com.linkedin.davinci.blobtransfer.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.blobtransfer.BlobTransferPartitionMetadata;
import com.linkedin.davinci.blobtransfer.BlobTransferPayload;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.IncrementalPushReplicaStatus;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
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
  private Supplier<VeniceNotifier> veniceNotifierSupplier;

  public P2PMetadataTransferHandler(
      StorageMetadataService storageMetadataService,
      String baseDir,
      String storeName,
      int version,
      int partition,
      BlobTransferTableFormat tableFormat,
      Supplier<VeniceNotifier> veniceNotifierSupplier) {
    this.storageMetadataService = storageMetadataService;
    this.veniceNotifierSupplier = veniceNotifierSupplier;
    this.payload = new BlobTransferPayload(baseDir, storeName, version, partition, tableFormat);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
    LOGGER.info(
        "Received metadata response from remote peer for {}",
        Utils.getReplicaId(payload.getTopicName(), payload.getPartition()));

    processMetadata(msg);

    LOGGER.info(
        "Successfully processed metadata response from remote peer for {} with metadata {}",
        Utils.getReplicaId(payload.getTopicName(), payload.getPartition()),
        metadata);
  }

  private void processMetadata(FullHttpResponse msg) throws IOException {
    if (!msg.status().equals(HttpResponseStatus.OK)) {
      throw new VeniceException("Failed to fetch metadata from remote peer. Response: " + msg.status());
    }

    ByteBuf content = msg.content();
    byte[] metadataBytes = new byte[content.readableBytes()];
    LOGGER.info(
        "Received metadata from remote peer for {} with size {}. ",
        Utils.getReplicaId(payload.getTopicName(), payload.getPartition()),
        metadataBytes.length);
    // verify the byte size of metadata
    if (metadataBytes.length != Long.parseLong(msg.headers().get(HttpHeaderNames.CONTENT_LENGTH))) {
      throw new VeniceException("Metadata byte size mismatch for topic " + payload.getTopicName());
    }

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
    LOGGER.info(
        "Start updating store partition metadata for {}",
        Utils.getReplicaId(transferredPartitionMetadata.topicName, transferredPartitionMetadata.partitionId));
    OffsetRecord transferredOffsetRecord =
        new OffsetRecord(transferredPartitionMetadata.offsetRecord.array(), partitionStateSerializer, null);
    // 1. update the offset incremental push job information
    updateIncrementalPushInfoToStore(transferredOffsetRecord, transferredPartitionMetadata);
    // 2. update the offset record in storage service
    storageMetadataService
        .put(transferredPartitionMetadata.topicName, transferredPartitionMetadata.partitionId, transferredOffsetRecord);
    // 3. update the metadata SVS
    updateStorageVersionState(storageMetadataService, transferredPartitionMetadata);
  }

  /**
   * Update the incremental push info to push status store from the transferred offset record trackingIncrementalPushStatus
   * @param offsetRecord
   * @param transferredPartitionMetadata
   */
  public void updateIncrementalPushInfoToStore(
      OffsetRecord offsetRecord,
      BlobTransferPartitionMetadata transferredPartitionMetadata) {
    String storeName = Version.parseStoreFromKafkaTopicName(transferredPartitionMetadata.getTopicName());
    int partitionId = transferredPartitionMetadata.getPartitionId();
    String kafkaTopic = transferredPartitionMetadata.getTopicName();
    Map<String, IncrementalPushReplicaStatus> incPushVersionToStatusMap =
        offsetRecord.getTrackingIncrementalPushStatus();

    if (incPushVersionToStatusMap == null || incPushVersionToStatusMap.isEmpty()) {
      LOGGER.info(
          "No incremental push info to update to push status store for {}, partition: {}",
          storeName,
          partitionId);
      return;
    }

    VeniceNotifier veniceNotifier = getVeniceNotifier();
    if (veniceNotifier == null) {
      LOGGER.error(
          "VeniceNotifier is not available, cannot write incremental push status for replica: {}",
          Utils.getReplicaId(kafkaTopic, partitionId));
      return;
    }

    // update the incremental push info to push status store per inc push version
    for (Map.Entry<String, IncrementalPushReplicaStatus> entry: incPushVersionToStatusMap.entrySet()) {
      String incPushVersion = entry.getKey();
      IncrementalPushReplicaStatus replicaStatus = entry.getValue();
      writeIncrementalPushStatusToPushStatusStore(
          veniceNotifier,
          kafkaTopic,
          partitionId,
          incPushVersion,
          replicaStatus);
    }

    LOGGER.info(
        "Successfully updated incremental push info to push status store for {}, partition: {}, incPushVersionToStatusMap: {}",
        storeName,
        partitionId,
        incPushVersionToStatusMap);
  }

  /**
   * Helper method to safely get VeniceNotifier from supplier
   * @return VeniceNotifier instance or null if not available
   */
  private VeniceNotifier getVeniceNotifier() {
    if (veniceNotifierSupplier == null) {
      return null;
    }

    try {
      return veniceNotifierSupplier.get();
    } catch (Exception e) {
      return null;
    }
  }

  private void writeIncrementalPushStatusToPushStatusStore(
      VeniceNotifier veniceNotifier,
      String kafkaTopic,
      int partitionId,
      String incPushVersion,
      IncrementalPushReplicaStatus incrementalPushReplicaStatus) {

    ExecutionStatus executionStatus = ExecutionStatus.valueOf(incrementalPushReplicaStatus.status);

    // Notify based on execution status
    if (executionStatus == ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED) {
      veniceNotifier.startOfIncrementalPushReceived(kafkaTopic, partitionId, null, incPushVersion);
    } else if (executionStatus == ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED) {
      veniceNotifier.endOfIncrementalPushReceived(kafkaTopic, partitionId, null, incPushVersion);
    } else {
      LOGGER.warn(
          "Unexpected execution status {} for incremental push. Replica: {}, inc push version: {}",
          executionStatus,
          Utils.getReplicaId(kafkaTopic, partitionId),
          incPushVersion);
    }
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
