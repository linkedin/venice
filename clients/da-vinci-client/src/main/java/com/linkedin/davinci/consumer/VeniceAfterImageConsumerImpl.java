package com.linkedin.davinci.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
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
  private Thread versionSwapDetectionThread;

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
    versionSwapDetectionThread = new VersionSwapDetectionThread();
    internalSeekConsumer = seekConsumer;
    versionSwapDetectionIntervalTimeInMs = changelogClientConfig.getVersionSwapDetectionIntervalTimeInMs();
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    if (!versionSwapDetectionThread.isAlive()) {
      versionSwapDetectionThread.start();
    }
    return internalPoll(timeoutInMs, "");
  }

  @Override
  public CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps) {
    return internalSeekToTimestamps(timestamps, "");
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
          this.seekToCheckpoint(checkpoints).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new VeniceException(
              "Seek to End of Push Failed for store: " + storeName + " partitions: " + partitions.toString(),
              e);
        }
      }
      return null;
    });
  }

  @Override
  protected CompletableFuture<Void> internalSeek(
      Set<Integer> partitions,
      PubSubTopic targetTopic,
      SeekFunction seekAction) {
    if (!versionSwapDetectionThread.isAlive()) {
      versionSwapDetectionThread.start();
    }
    return super.internalSeek(partitions, targetTopic, seekAction);
  }

  private class VersionSwapDetectionThread extends Thread {
    VersionSwapDetectionThread() {
      super("Version-Swap-Detection-Thread");
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          TimeUnit.MILLISECONDS.sleep(versionSwapDetectionIntervalTimeInMs);
        } catch (InterruptedException e) {
          // We've received an interrupt which is to be expected, so we'll just leave the loop and log
          break;
        }

        // Check the current version of the server
        storeRepository.refresh();
        int currentVersion = storeRepository.getStore(storeName).getCurrentVersion();

        // Check the current ingested version
        Set<PubSubTopicPartition> subscriptions = pubSubConsumer.getAssignment();
        if (subscriptions.isEmpty()) {
          continue;
        }
        int maxVersion = -1;
        for (PubSubTopicPartition topicPartition: subscriptions) {
          int version = Version.parseVersionFromVersionTopicName(topicPartition.getPubSubTopic().getName());
          if (version >= maxVersion) {
            maxVersion = version;
          }
        }

        // Seek to end of push
        if (currentVersion != maxVersion) {
          // get current subscriptions and seek to endOfPush
          Set<Integer> partitions = new HashSet<>();
          for (PubSubTopicPartition partitionSubscription: subscriptions) {
            partitions.add(partitionSubscription.getPartitionNumber());
          }
          try {
            seekToEndOfPush(partitions).get();
          } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(
                "Seek to End of Push Failed for store: " + storeName + " partitions: " + partitions + " will retry...",
                e);
          }
        }
      }
      LOGGER.info("VersionSwapDetectionThread thread interrupted!  Shutting down...");
    }
  }
}
