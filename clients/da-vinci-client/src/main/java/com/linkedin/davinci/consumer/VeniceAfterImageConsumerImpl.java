package com.linkedin.davinci.consumer;

import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAfterImageConsumerImpl<K, V> extends VeniceChangelogConsumerImpl<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAfterImageConsumerImpl.class);
  // This consumer is used to find EOP messages without impacting consumption by other subscriptions. It's only used
  // in the context of seeking to EOP in the event of the user calling that seek or a version push.
  // TODO: We shouldn't use this in the long run. Once the EOP position is queryable from venice and version
  // swap is produced to VT, then we should remove this as it's no longer needed.
  private final Lazy<PubSubConsumerAdapter> internalSeekConsumer;
  private final AtomicBoolean versionSwapThreadScheduled = new AtomicBoolean(false);
  /*
   * Used to track if the version swap thread encountered an exception. If so, the exception will be reflected
   * in the next call to poll to prevent silent thread termination and fail the process.
   */
  private final AtomicReference<Exception> versionSwapThreadException = new AtomicReference<>();
  private final VersionSwapDataChangeListener<K, V> versionSwapListener;

  public VeniceAfterImageConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter consumer,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory) {
    this(
        changelogClientConfig,
        consumer,
        Lazy.of(
            () -> VeniceChangelogConsumerClientFactory.getPubSubConsumer(
                changelogClientConfig,
                pubSubMessageDeserializer,
                changelogClientConfig.getStoreName() + "-" + "internal")),
        pubSubMessageDeserializer,
        veniceChangelogConsumerClientFactory);
  }

  protected VeniceAfterImageConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter consumer,
      Lazy<PubSubConsumerAdapter> seekConsumer,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory) {
    super(changelogClientConfig, consumer, pubSubMessageDeserializer, veniceChangelogConsumerClientFactory);
    internalSeekConsumer = seekConsumer;
    versionSwapListener = new VersionSwapDataChangeListener<K, V>(
        this,
        storeRepository,
        storeName,
        changelogClientConfig.getConsumerName(),
        this.changeCaptureStats,
        changelogClientConfig.isVersionSwapByControlMessageEnabled());
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    Exception versionSwapException = versionSwapThreadException.get();
    if (versionSwapException != null) {
      throw new VeniceException(
          "Version Swap failed for store: " + storeName + " due to exception:",
          versionSwapException);
    }

    return internalPoll(timeoutInMs);
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    if (timestamps.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return internalSeekToTimestamps(timestamps);
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
                PubSubSymbolicPosition.LATEST,
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
        if (consumerAdapter.positionDifference(topicPartition, heartbeatTimestampPosition, eopPosition) > 0) {
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

  /**
   * Similar to {@link #internalSeekToEndOfPush} exception in addition to finding the EOP of each partition we will also
   * be looking for the first relevant version swap. This can also be optimized later for a faster find.
   */
  @Override
  protected CompletableFuture<Void> internalFindNewVersionCheckpoints(
      String oldVersionTopic,
      String newVersionTopic,
      long generationId,
      Set<Integer> partitions) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.supplyAsync(() -> {
      boolean lockAcquired = false;
      Map<Integer, VeniceChangeCoordinate> checkpoints = new HashMap<>();
      Map<Integer, VeniceChangeCoordinate> eopCheckpoints = new HashMap<>();
      try {
        synchronized (internalSeekConsumer) {
          PubSubConsumerAdapter consumerAdapter = internalSeekConsumer.get();
          consumerAdapter.batchUnsubscribe(consumerAdapter.getAssignment());
          Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledResults;
          Map<Integer, Boolean> versionSwapConsumedPerPartitionMap = new HashMap<>();
          for (Integer partition: partitions) {
            versionSwapConsumedPerPartitionMap.put(partition, false);
          }
          List<PubSubTopicPartition> topicPartitionList = getPartitionListToSubscribe(
              partitions,
              Collections.EMPTY_SET,
              pubSubTopicRepository.getTopic(newVersionTopic));

          for (PubSubTopicPartition topicPartition: topicPartitionList) {
            consumerAdapter.subscribe(topicPartition, PubSubSymbolicPosition.EARLIEST);
          }

          // Poll until we receive the desired version swap message in the new version topic for each partition
          LOGGER.info(
              "Polling for version swap messages in: {} with generation id: {} for partitions: {}",
              newVersionTopic,
              generationId,
              partitions);
          while (!areAllTrue(versionSwapConsumedPerPartitionMap.values())) {
            polledResults = consumerAdapter.poll(5000L);
            for (Map.Entry<PubSubTopicPartition, List<DefaultPubSubMessage>> entry: polledResults.entrySet()) {
              PubSubTopicPartition pubSubTopicPartition = entry.getKey();
              List<DefaultPubSubMessage> messageList = entry.getValue();
              for (DefaultPubSubMessage message: messageList) {
                if (message.getKey().isControlMessage()) {
                  ControlMessage controlMessage = (ControlMessage) message.getValue().getPayloadUnion();
                  ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
                  if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
                    VeniceChangeCoordinate eopCoordinate = new VeniceChangeCoordinate(
                        pubSubTopicPartition.getPubSubTopic().getName(),
                        message.getPosition(),
                        pubSubTopicPartition.getPartitionNumber());
                    eopCheckpoints.put(pubSubTopicPartition.getPartitionNumber(), eopCoordinate);
                    LOGGER.info(
                        "Found EOP for version swap message with generation id: {} for partition: {}",
                        generationId,
                        pubSubTopicPartition.getPartitionNumber());
                    // We continue to poll until we find the corresponding version swap which should be after EOP
                  } else if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
                    VersionSwap versionSwap = (VersionSwap) controlMessage.getControlMessageUnion();
                    // In theory just matching the generation id and source region should be sufficient but just to be
                    // safe we will match all fields
                    if (versionSwap.getGenerationId() == generationId
                        && versionSwap.getSourceRegion().toString().equals(clientRegionName)
                        && oldVersionTopic.equals(versionSwap.getOldServingVersionTopic().toString())
                        && newVersionTopic.equals(versionSwap.getNewServingVersionTopic().toString())) {
                      versionSwapConsumedPerPartitionMap.put(pubSubTopicPartition.getPartitionNumber(), true);
                      VeniceChangeCoordinate coordinate = new VeniceChangeCoordinate(
                          pubSubTopicPartition.getPubSubTopic().getName(),
                          message.getPosition(),
                          pubSubTopicPartition.getPartitionNumber());
                      checkpoints.put(pubSubTopicPartition.getPartitionNumber(), coordinate);
                      // We are done with this partition
                      consumerAdapter.unSubscribe(pubSubTopicPartition);
                      LOGGER.info(
                          "Found corresponding version swap message with generation id: {} for partition: {}",
                          generationId,
                          pubSubTopicPartition.getPartitionNumber());
                      break;
                    }
                  }
                }
              }
            }
          }
          LOGGER.info(
              "Found all version swap messages in: {} with generation id: {} for partitions: {}",
              newVersionTopic,
              generationId,
              partitions);
        }
        // We cannot change the subscription here because the consumer might not finish polling all the messages in the
        // old version topic yet. We can acquire the lock and update the VersionSwapMessageState.
        subscriptionLock.writeLock().lock();
        lockAcquired = true;
        versionSwapMessageState.setNewTopicVersionSwapCheckpoints(checkpoints);
        versionSwapMessageState.setNewTopicEOPCheckpoints(eopCheckpoints);
      } finally {
        if (lockAcquired) {
          subscriptionLock.writeLock().unlock();
        }
      }
      return null;
    }, seekExecutorService);
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
            consumerAdapter.subscribe(topicPartition, PubSubSymbolicPosition.EARLIEST);
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
                getLastHeartbeatPerPartition(),
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

  private boolean areAllTrue(Collection<Boolean> booleanCollections) {
    for (Boolean b: booleanCollections) {
      if (!b) {
        return false;
      }
    }
    return true;
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

  @Override
  public void close() {
    super.close();
    if (internalSeekConsumer.isPresent()) {
      internalSeekConsumer.get().close();
    }
    storeRepository.unregisterStoreDataChangedListener(versionSwapListener);
  }

  /**
   * Used by {@link VersionSwapDataChangeListener} to propagate version swap exceptions to prevent silent thread
   * termination.
   */
  protected void handleVersionSwapFailure(Exception error) {
    versionSwapThreadException.set(error);
  }
}
