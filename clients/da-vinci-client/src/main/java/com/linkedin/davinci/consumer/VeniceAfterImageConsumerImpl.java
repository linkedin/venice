package com.linkedin.davinci.consumer;

import static com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory.getConsumer;
import static com.linkedin.venice.pubsub.api.PubSubSymbolicPosition.LATEST;

import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAfterImageConsumerImpl<K, V> extends VeniceChangelogConsumerImpl<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAfterImageConsumerImpl.class);
  // This consumer is used to find EOP messages without impacting consumption by other subscriptions. It's only used
  // in the context of seeking to EOP in the event of the user calling that seek or a version push.
  // TODO: We shouldn't use this in the long run. Once the EOP position is queryable from venice and version
  // swap is produced to VT, then we should remove this as it's no longer needed.
  final private Lazy<PubSubConsumerAdapter> internalSeekConsumer;
  AtomicBoolean versionSwapThreadScheduled = new AtomicBoolean(false);
  private final VersionSwapDataChangeListener<K, V> versionSwapListener;

  public VeniceAfterImageConsumerImpl(ChangelogClientConfig changelogClientConfig, PubSubConsumerAdapter consumer) {
    this(
        changelogClientConfig,
        consumer,
        Lazy.of(
            () -> getConsumer(
                changelogClientConfig.getConsumerProperties(),
                changelogClientConfig.getStoreName() + "-" + "internal")));
  }

  protected VeniceAfterImageConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter consumer,
      Lazy<PubSubConsumerAdapter> seekConsumer) {
    super(changelogClientConfig, consumer);
    internalSeekConsumer = seekConsumer;
    versionSwapListener = new VersionSwapDataChangeListener<K, V>(
        this,
        storeRepository,
        storeName,
        changelogClientConfig.getConsumerName());
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    try {
      return internalPoll(timeoutInMs, "");
    } catch (UnknownTopicOrPartitionException ex) {
      LOGGER.error("Caught unknown Topic exception, will attempt repair and retry: ", ex);
      return internalPoll(timeoutInMs, "");
    }
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    if (timestamps.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return internalSeekToTimestamps(timestamps, "");
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    try {
      storeRepository.subscribe(storeName);
    } catch (InterruptedException e) {
      throw new VeniceException("Failed to start bootstrapping changelog consumer with error:", e);
    }
    if (!versionSwapThreadScheduled.get()) {
      // schedule the version swap thread and set up the callback listener
      this.storeRepository.registerStoreDataChangedListener(versionSwapListener);
      versionSwapThreadScheduled.set(true);
    }
    return super.subscribe(partitions);
  }

  @Override
  public CompletableFuture<Void> seekToTail(Set<Integer> partitions) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return internalSeekToTail(partitions, "");
  }

  protected static void adjustSeekCheckPointsBasedOnHeartbeats(
      Map<Integer, VeniceChangeCoordinate> checkpoints,
      Map<Integer, Long> currentVersionLastHeartbeat,
      PubSubConsumerAdapter consumerAdapter,
      List<PubSubTopicPartition> topicPartitionList) {
    for (PubSubTopicPartition topicPartition: topicPartitionList) {
      Long currentVersionTimestamp = currentVersionLastHeartbeat.get(topicPartition.getPartitionNumber());
      if (currentVersionTimestamp == null) {
        LOGGER.warn("No heartbeat checkpoint found for partition: {}", topicPartition.getPartitionNumber());
        continue;
      }
      PubSubPosition heartbeatTimestampPosition =
          consumerAdapter.getPositionByTimestamp(topicPartition, currentVersionTimestamp);
      if (checkpoints.get(topicPartition.getPartitionNumber()) == null && heartbeatTimestampPosition == null) {
        LOGGER.warn(
            "No EOP checkpoint OR heartbeat position found for partition: {} seeking to tail",
            topicPartition.getPartitionNumber());
        checkpoints.put(
            topicPartition.getPartitionNumber(),
            new VeniceChangeCoordinate(
                topicPartition.getPubSubTopic().getName(),
                LATEST,
                topicPartition.getPartitionNumber()));
      } else if (checkpoints.get(topicPartition.getPartitionNumber()) == null) {
        LOGGER.warn("No EOP checkpoint found for partition: {}", topicPartition.getPartitionNumber());
        checkpoints.put(
            topicPartition.getPartitionNumber(),
            new VeniceChangeCoordinate(
                topicPartition.getPubSubTopic().getName(),
                heartbeatTimestampPosition,
                topicPartition.getPartitionNumber()));
      } else if (heartbeatTimestampPosition == null) {
        LOGGER.warn(
            "Null heartbeat position for partition: {} timestamp: {} starting from EOP!",
            topicPartition.getPartitionNumber(),
            currentVersionTimestamp);
        // No need to do anything here, we already have the EOP checkpoint, so we'll default to that
      } else {
        PubSubPosition eopPosition = checkpoints.get(topicPartition.getPartitionNumber()).getPosition();
        if (heartbeatTimestampPosition.comparePosition(eopPosition) > 0) {
          checkpoints.put(
              topicPartition.getPartitionNumber(),
              new VeniceChangeCoordinate(
                  topicPartition.getPubSubTopic().getName(),
                  heartbeatTimestampPosition,
                  topicPartition.getPartitionNumber()));
        }
      }
    }
  }

  protected CompletableFuture<Void> internalSeekToEndOfPush(
      Set<Integer> partitions,
      PubSubTopic targetTopic,
      boolean trackHeartbeats) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.supplyAsync(() -> {
      boolean lockAcquired = false;
      Map<Integer, VeniceChangeCoordinate> checkpoints = new HashMap<>();
      try {
        // TODO: This implementation basically just scans the version topic until it finds the EOP message. The
        // approach
        // we'd like to do is instead add the offset of the EOP message in the VT, and then just seek to that offset.
        // We'll do that in a future patch.

        // We need to get the internal consumer as we have to intercept the control messages that we would normally
        // filter out from the user
        synchronized (internalSeekConsumer) {
          PubSubConsumerAdapter consumerAdapter = internalSeekConsumer.get();
          consumerAdapter.batchUnsubscribe(consumerAdapter.getAssignment());
          List<PubSubTopicPartition> topicPartitionList =
              getPartitionListToSubscribe(partitions, Collections.EMPTY_SET, targetTopic);

          for (PubSubTopicPartition topicPartition: topicPartitionList) {
            consumerAdapter.subscribe(topicPartition, OffsetRecord.LOWEST_OFFSET_LAG);
          }
          Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledResults;
          Map<Integer, Boolean> endOfPushConsumedPerPartitionMap = new HashMap<>();

          // Initialize map with all false entries for each partition
          for (Integer partition: partitions) {
            endOfPushConsumedPerPartitionMap.put(partition, false);
          }

          // poll until we get EOP for all partitions
          LOGGER.info("Polling for EOP messages for partitions: " + partitions.toString());
          while (true) {
            polledResults = consumerAdapter.poll(5000L);
            // Loop through all polled messages
            for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: polledResults.entrySet()) {
              PubSubTopicPartition pubSubTopicPartition = entry.getKey();
              List<DefaultPubSubMessage> messageList = entry.getValue();
              for (DefaultPubSubMessage message: messageList) {
                if (message.getKey().isControlMessage()) {
                  ControlMessage controlMessage = (ControlMessage) message.getValue().getPayloadUnion();
                  ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
                  if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
                    LOGGER.info("Found EOP message for partition: " + pubSubTopicPartition.getPartitionNumber());
                    // note down the partition and offset and mark that we've got the thing
                    endOfPushConsumedPerPartitionMap.put(pubSubTopicPartition.getPartitionNumber(), true);
                    VeniceChangeCoordinate coordinate = new VeniceChangeCoordinate(
                        pubSubTopicPartition.getPubSubTopic().getName(),
                        message.getPosition(),
                        pubSubTopicPartition.getPartitionNumber());
                    checkpoints.put(pubSubTopicPartition.getPartitionNumber(), coordinate);
                    // No need to look at the rest of the messages for this partition that we might have polled
                    consumerAdapter.unSubscribe(pubSubTopicPartition);
                    break;
                  }
                }
              }
            }
            if (endOfPushConsumedPerPartitionMap.values().stream().allMatch(e -> e)) {
              LOGGER.info("Found EOP messages for all partitions: " + partitions.toString());
              // We polled all EOP messages, stop polling!
              break;
            }
          }
          if (trackHeartbeats) {
            // One last step. We track heartbeats for each partition and check their positions. The time recorded in
            // the
            // last received heartbeat is absolutely the earliest possible time for that heartbeat to be processed
            // on a server. If the message nearest that heartbeat in the new version is at on offset higher then the
            // EOP message, then we swap it out in order to play less events back to the user.
            // first, check and see if all partitions are already after EOP
            adjustSeekCheckPointsBasedOnHeartbeats(
                checkpoints,
                new HashMap<>(currentVersionLastHeartbeat),
                consumerAdapter,
                topicPartitionList);
          }
          LOGGER.info(
              "Seeking to EOP for partitions: " + partitions.toString() + " for version topic: "
                  + targetTopic.getName());
          subscriptionLock.writeLock().lock();
          lockAcquired = true;
          this.synchronousSeekToCheckpoint(new HashSet<>(checkpoints.values()));
          LOGGER.info(
              "Seeked to EOP for partitions: " + partitions.toString() + " for version topic: "
                  + targetTopic.getName());
        }
      } finally {
        if (lockAcquired) {
          subscriptionLock.writeLock().unlock();
        }
      }
      return null;
    }, seekExecutorService);
  }

  @Override
  public CompletableFuture<Void> seekToEndOfPush(Set<Integer> partitions) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return internalSeekToEndOfPush(partitions, getCurrentServingVersionTopic(), false);
  }

  public boolean subscribed() {
    return isSubscribed.get();
  }

  @Override
  protected CompletableFuture<Void> internalSeek(
      Set<Integer> partitions,
      PubSubTopic targetTopic,
      SeekFunction seekAction) {
    return super.internalSeek(partitions, targetTopic, seekAction);
  }

  @Override
  public void setStoreRepository(NativeMetadataRepositoryViewAdapter repository) {
    super.setStoreRepository(repository);
    versionSwapListener.setStoreRepository(repository);
  }
}
