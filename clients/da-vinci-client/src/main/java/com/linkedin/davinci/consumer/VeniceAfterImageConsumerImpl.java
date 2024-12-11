package com.linkedin.davinci.consumer;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAfterImageConsumerImpl<K, V> extends VeniceChangelogConsumerImpl<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAfterImageConsumerImpl.class);
  // 10 Minute default
  protected long versionSwapDetectionIntervalTimeInMs;
  // This consumer is used to find EOP messages without impacting consumption by other subscriptions. It's only used
  // in the context of seeking to EOP in the event of the user calling that seek or a version push.
  // TODO: We shouldn't use this in the long run. Once the EOP position is queryable from venice and version
  // swap is produced to VT, then we should remove this as it's no longer needed.
  final private Lazy<VeniceChangelogConsumerImpl<K, V>> internalSeekConsumer;
  private final ScheduledExecutorService versionSwapExecutorService = Executors.newSingleThreadScheduledExecutor();
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
    versionSwapDetectionIntervalTimeInMs = changelogClientConfig.getVersionSwapDetectionIntervalTimeInMs();
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
      storeRepository.refresh();
      versionSwapListener.handleStoreChanged(null);
      return internalPoll(timeoutInMs, "");
    }
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    return internalSeekToTimestamps(timestamps, "");
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    if (!versionSwapThreadScheduled.get()) {
      // schedule the version swap thread and set up the callback listener
      this.storeRepository.registerStoreDataChangedListener(versionSwapListener);
      versionSwapExecutorService.scheduleAtFixedRate(
          new VersionSwapDetectionThread(),
          versionSwapDetectionIntervalTimeInMs,
          versionSwapDetectionIntervalTimeInMs,
          TimeUnit.MILLISECONDS);
      versionSwapThreadScheduled.set(true);
    }
    return super.subscribe(partitions);
  }

  @Override
  public CompletableFuture<Void> seekToTail(Set<Integer> partitions) {
    return internalSeekToTail(partitions, "");
  }

  @Override
  public CompletableFuture<Void> seekToEndOfPush(Set<Integer> partitions) {
    return CompletableFuture.supplyAsync(() -> {
      synchronized (internalSeekConsumer) {
        try {
          // TODO: This implementation basically just scans the version topic until it finds the EOP message. The
          // approach
          // we'd like to do is instead add the offset of the EOP message in the VT, and then just seek to that offset.
          // We'll do that in a future patch.
          internalSeekConsumer.get().subscribe(partitions).get();

          // We need to get the internal consumer as we have to intercept the control messages that we would normally
          // filter out from the user
          PubSubConsumerAdapter consumerAdapter = internalSeekConsumer.get().getPubSubConsumer();

          Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledResults;
          Map<Integer, Boolean> endOfPushConsumedPerPartitionMap = new HashMap<>();
          Set<VeniceChangeCoordinate> checkpoints = new HashSet<>();

          // Initialize map with all false entries for each partition
          for (Integer partition: partitions) {
            endOfPushConsumedPerPartitionMap.put(partition, false);
          }

          // poll until we get EOP for all partitions
          synchronized (consumerAdapter) {
            while (true) {
              polledResults = consumerAdapter.poll(5000L);
              // Loop through all polled messages
              for (Map.Entry<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> entry: polledResults
                  .entrySet()) {
                PubSubTopicPartition pubSubTopicPartition = entry.getKey();
                List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> messageList = entry.getValue();
                for (PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> message: messageList) {
                  if (message.getKey().isControlMessage()) {
                    ControlMessage controlMessage = (ControlMessage) message.getValue().getPayloadUnion();
                    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
                    if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
                      // note down the partition and offset and mark that we've got the thing
                      endOfPushConsumedPerPartitionMap.put(pubSubTopicPartition.getPartitionNumber(), true);
                      VeniceChangeCoordinate coordinate = new VeniceChangeCoordinate(
                          pubSubTopicPartition.getPubSubTopic().getName(),
                          new ApacheKafkaOffsetPosition(message.getOffset()),
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
                // We polled all EOP messages, stop polling!
                break;
              }
            }
          }
          this.seekToCheckpoint(checkpoints).get();
        } catch (InterruptedException | ExecutionException | VeniceCoordinateOutOfRangeException e) {
          throw new VeniceException(
              "Seek to End of Push Failed for store: " + storeName + " partitions: " + partitions.toString(),
              e);
        }
      }
      return null;
    });
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

  private class VersionSwapDetectionThread implements Runnable {
    @Override
    public void run() {
      // the purpose of this thread is to just keep polling just in case something goes wrong at time of the store
      // repository change.
      versionSwapListener.handleStoreChanged(null);
    }
  }
}
