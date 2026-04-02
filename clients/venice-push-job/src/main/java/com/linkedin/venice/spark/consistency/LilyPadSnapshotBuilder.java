package com.linkedin.venice.spark.consistency;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Builds a {@link LilyPadUtils.Snapshot} from a PubSub partition split by reading records
 * through a {@link PubSubSplitIterator} and wrapping positions in {@link ComparablePubSubPosition}.
 *
 * <p>This class is the bridge between the PubSub infrastructure and the generic lily-pad algorithm
 * in {@link LilyPadUtils}. It is the only class that depends on PubSub types for snapshot construction.
 */
public final class LilyPadSnapshotBuilder {
  private LilyPadSnapshotBuilder() {
  }

  /**
   * Iterates over a pre-built {@link PubSubSplitIterator} and returns a per-key history of PUT and DELETE
   * records in the order they appeared in the VT partition.
   *
   * <p>The caller is responsible for constructing the iterator (including consumer creation and
   * position discovery) and for closing it after this method returns.
   *
   * <p>The running high-watermark is computed in VT position order, so each {@link LilyPadUtils.KeyRecord}'s
   * {@code highWatermark} reflects global RT progress up to that point.
   *
   * @param iterator                   a ready-to-read iterator over a single VT partition split
   * @param topicManager               TopicManager used for position comparison (max watermark)
   * @param pubSubPositionDeserializer deserializer for upstream PubSubPosition bytes
   * @param numberOfRegions            total number of regions in the AA topology (determines position vector size)
   * @return {@link LilyPadUtils.Snapshot} containing the per-key record map and the final partition high-watermark
   */
  public static LilyPadUtils.Snapshot<ComparablePubSubPosition> buildSnapshot(
      PubSubSplitIterator iterator,
      TopicManager topicManager,
      PubSubPositionDeserializer pubSubPositionDeserializer,
      int numberOfRegions) {
    Map<Long, List<LilyPadUtils.KeyRecord<ComparablePubSubPosition>>> snapshot = new HashMap<>();
    List<ComparablePubSubPosition> runningHighWatermark = new ArrayList<>(Collections.nCopies(numberOfRegions, null));
    PubSubTopicPartition topicPartition = iterator.getTopicPartition();

    try {
      PubSubSplitIterator.PubSubInputRecord record;
      while ((record = iterator.next()) != null) {
        DefaultPubSubMessage msg = record.getPubSubMessage();
        KafkaMessageEnvelope kme = msg.getValue();
        MessageType messageType = MessageType.valueOf(kme);
        if (messageType != MessageType.PUT && messageType != MessageType.DELETE) {
          continue;
        }
        LeaderMetadata leaderMetadata = kme.leaderMetadataFooter;

        if (leaderMetadata == null) {
          continue;
        }
        int regionId = leaderMetadata.upstreamKafkaClusterId;
        PubSubPosition rawPosition = PubSubUtil.deserializePositionWithOffsetFallback(
            leaderMetadata.upstreamPubSubPosition,
            leaderMetadata.upstreamOffset,
            pubSubPositionDeserializer);
        ComparablePubSubPosition upstreamPosition =
            new ComparablePubSubPosition(rawPosition, topicManager, topicPartition);
        ComparablePubSubPosition currentHw = runningHighWatermark.get(regionId);
        if (currentHw == null || upstreamPosition.compareTo(currentHw) > 0) {
          runningHighWatermark.set(regionId, upstreamPosition);
        }
        long keyHash = ByteUtils.hash64(msg.getKey().getKey());
        // null valueHash represents a tombstone (DELETE)
        Integer valueHash = messageType == MessageType.PUT
            ? Arrays.hashCode(ByteUtils.extractByteArray(((Put) kme.payloadUnion).putValue))
            : null;
        List<LilyPadUtils.KeyRecord<ComparablePubSubPosition>> history =
            snapshot.computeIfAbsent(keyHash, k -> new ArrayList<>());
        // Inherit previous position vector for this key, then update only the current region.
        List<ComparablePubSubPosition> positionVector = history.isEmpty()
            ? new ArrayList<>(Collections.nCopies(numberOfRegions, null))
            : new ArrayList<>(history.get(history.size() - 1).upstreamRTPosition);
        positionVector.set(regionId, upstreamPosition);
        history.add(
            new LilyPadUtils.KeyRecord<>(
                valueHash,
                positionVector,
                new ArrayList<>(runningHighWatermark),
                kme.producerMetadata.logicalTimestamp,
                new ComparablePubSubPosition(msg.getPosition(), topicManager, topicPartition)));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to scan " + iterator.getTopicPartition(), e);
    }
    return new LilyPadUtils.Snapshot<>(snapshot, runningHighWatermark);
  }

}
