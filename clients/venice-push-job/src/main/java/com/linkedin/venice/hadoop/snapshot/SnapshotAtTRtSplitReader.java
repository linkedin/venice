package com.linkedin.venice.hadoop.snapshot;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Reads one {@link SnapshotAtTRtSplit} (a bounded range of one RT topic-partition in one region) into normalized
 * {@link SnapshotAtTRtRecord}s. Each distributed task runs this for its own split, so no task ever holds more than a
 * single partition-range — this is what makes the distributed merge memory-bounded instead of draining all RT into a
 * single process. Reuses {@link PubSubSplitIterator} for the bounded, control-message-skipping read and the shared
 * {@link SnapshotAtTRtRecordDecoder} for decoding, so a split's records match the single-process reader exactly.
 */
public class SnapshotAtTRtSplitReader {
  /**
   * @param rtSplit the split to read (carries the broker, coloId, and the partition's start/end positions)
   * @param baseProps the job's pubsub client config; the split's broker is overlaid onto a copy of it
   * @param cutoffTimestampMs include only records with write timestamp &le; this; {@code <= 0} means no bound
   */
  public List<SnapshotAtTRtRecord> read(
      SnapshotAtTRtSplit rtSplit,
      VeniceProperties baseProps,
      long cutoffTimestampMs) {
    VeniceProperties consumerProps = withBroker(baseProps, rtSplit.getBroker());
    PubSubClientsFactory clientsFactory = new PubSubClientsFactory(consumerProps);
    PubSubConsumerAdapterContext consumerContext = new PubSubConsumerAdapterContext.Builder()
        .setConsumerName("snapshot-at-t-rt-split-reader-colo-" + rtSplit.getColoId())
        .setPubSubBrokerAddress(rtSplit.getBroker())
        .setVeniceProperties(consumerProps)
        .setPubSubTopicRepository(new PubSubTopicRepository())
        .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
        .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(consumerProps))
        .build();

    List<SnapshotAtTRtRecord> records = new ArrayList<>();
    try (PubSubConsumerAdapter consumer = clientsFactory.getConsumerAdapterFactory().create(consumerContext);
        PubSubSplitIterator splitIterator = new PubSubSplitIterator(consumer, rtSplit.getSplit(), false)) {
      PubSubSplitIterator.PubSubInputRecord record;
      while ((record = splitIterator.next()) != null) {
        SnapshotAtTRtRecord rtRecord =
            SnapshotAtTRtRecordDecoder.decode(record.getPubSubMessage(), rtSplit.getColoId(), cutoffTimestampMs);
        if (rtRecord != null) {
          records.add(rtRecord);
        }
      }
    } catch (IOException e) {
      throw new VeniceException("Failed to read snapshot-at-T RT split " + rtSplit, e);
    }
    return records;
  }

  /** A copy of {@code baseProps} pointed at the split's region broker. */
  private static VeniceProperties withBroker(VeniceProperties baseProps, String broker) {
    Properties properties = baseProps.toProperties();
    properties.setProperty(PUBSUB_BROKER_ADDRESS, broker);
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, broker);
    return new VeniceProperties(properties);
  }
}
