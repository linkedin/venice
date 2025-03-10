package com.linkedin.davinci.consumer;

import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
  final private Lazy<VeniceChangelogConsumerImpl<K, V>> internalSeekConsumer;
  AtomicBoolean versionSwapThreadScheduled = new AtomicBoolean(false);
  private final VersionSwapDataChangeListener<K, V> versionSwapListener;

  public VeniceAfterImageConsumerImpl(ChangelogClientConfig changelogClientConfig, PubSubConsumerAdapter consumer) {
    this(
        changelogClientConfig,
        consumer,
        Lazy.of(
            () -> new VeniceChangelogConsumerImpl<K, V>(
                changelogClientConfig,
                VeniceChangelogConsumerClientFactory.getConsumer(
                    changelogClientConfig.getConsumerProperties(),
                    changelogClientConfig.getStoreName() + "-" + "internal"))));
  }

  protected VeniceAfterImageConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      PubSubConsumerAdapter consumer,
      Lazy<VeniceChangelogConsumerImpl<K, V>> seekConsumer) {
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

  protected CompletableFuture<Void> internalSeekToEndOfPush(Set<Integer> partitions, PubSubTopic targetTopic) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.supplyAsync(() -> {
      synchronized (internalSeekConsumer) {
        try {
          // TODO: This implementation basically just scans the version topic until it finds the EOP message. The
          // approach
          // we'd like to do is instead add the offset of the EOP message in the VT, and then just seek to that offset.
          // We'll do that in a future patch.
          internalSeekConsumer.get().unsubscribeAll();
          internalSeekConsumer.get().internalSubscribe(partitions, targetTopic).get();

          // We need to get the internal consumer as we have to intercept the control messages that we would normally
          // filter out from the user
          PubSubConsumerAdapter consumerAdapter = internalSeekConsumer.get().getPubSubConsumer();

          Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledResults;
          Map<Integer, Boolean> endOfPushConsumedPerPartitionMap = new HashMap<>();
          Set<VeniceChangeCoordinate> checkpoints = new HashSet<>();

          // Initialize map with all false entries for each partition
          for (Integer partition: partitions) {
            endOfPushConsumedPerPartitionMap.put(partition, false);
          }

          // poll until we get EOP for all partitions
          LOGGER.info("Polling for EOP messages for partitions: " + partitions.toString());
          synchronized (consumerAdapter) {
            LOGGER.info("GOT LOCK");
            int counter = 0;
            while (true) {
              counter++;
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
                      checkpoints.add(coordinate);
                      Set<Integer> unsubSet = new HashSet<>();
                      unsubSet.add(pubSubTopicPartition.getPartitionNumber());
                      internalSeekConsumer.get().unsubscribe(unsubSet);
                      // No need to look at the rest of the messages for this partition that we might have polled
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
          }
          LOGGER.info("Seeking to EOP for partitions: " + partitions.toString());
          this.seekToCheckpoint(checkpoints).get();
          LOGGER.info("Seeked to EOP for partitions: " + partitions.toString());
        } catch (InterruptedException | ExecutionException | VeniceCoordinateOutOfRangeException e) {
          throw new VeniceException(
              "Seek to End of Push Failed for store: " + storeName + " partitions: " + partitions.toString(),
              e);
        }
      }
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> seekToEndOfPush(Set<Integer> partitions) {
    if (partitions.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return internalSeekToEndOfPush(partitions, getCurrentServingVersionTopic());
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
