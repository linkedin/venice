package com.linkedin.venice.ingestion.control;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MultiRegionRealTimeTopicSwitcher extends RealTimeTopicSwitcher {
  private static final Logger LOGGER = LogManager.getLogger(MultiRegionRealTimeTopicSwitcher.class);
  private static final int DEFAULT_BROADCAST_TIMEOUT_IN_SECONDS = 60;
  private final Map<String, String> allAASourceDataCenterBrokerAddressMap;
  private final String localDataCenterName;

  public MultiRegionRealTimeTopicSwitcher(
      TopicManager topicManager,
      VeniceWriterFactory localVeniceWriterFactory,
      VeniceProperties veniceProperties,
      PubSubTopicRepository pubSubTopicRepository,
      Map<String, String> activeActiveRealTimeSourceFabricBrokerUrlMap,
      String localDataCenterName) {
    super(topicManager, localVeniceWriterFactory, veniceProperties, pubSubTopicRepository);
    this.allAASourceDataCenterBrokerAddressMap = new HashMap<>(activeActiveRealTimeSourceFabricBrokerUrlMap);
    this.localDataCenterName = localDataCenterName;
  }

  @Override
  protected void broadcastVersionSwap(Version previousStoreVersion, Version nextStoreVersion, String topicName) {
    String storeName = previousStoreVersion.getStoreName();
    int partitionCount;
    if (topicName.equals(previousStoreVersion.kafkaTopicName())) {
      partitionCount = previousStoreVersion.getPartitionCount();
    } else {
      partitionCount = nextStoreVersion.getPartitionCount();
    }

    long generationId = getVersionSwapGenerationId();
    LOGGER.info(
        "Broadcasting Version Swap message with generation id: {}, source data center: {} to topic: {} for store: {} to {} partitions and {} data center(s)",
        generationId,
        localDataCenterName,
        topicName,
        storeName,
        partitionCount,
        allAASourceDataCenterBrokerAddressMap.size());
    ExecutorService regionExecutor = Executors.newFixedThreadPool(
        Math.max(1, allAASourceDataCenterBrokerAddressMap.size()),
        new DaemonThreadFactory("Version-Swap-" + storeName));
    Map<String, CompletableFuture<Void>> dataCenterBroadcastFutureMap = new HashMap<>();
    long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEFAULT_BROADCAST_TIMEOUT_IN_SECONDS);
    try {
      for (Map.Entry<String, String> entry: allAASourceDataCenterBrokerAddressMap.entrySet()) {
        String dataCenterName = entry.getKey();
        String brokerAddress = entry.getValue();
        dataCenterBroadcastFutureMap.put(
            dataCenterName,
            CompletableFuture.runAsync(
                () -> broadcastVersionSwapToDataCenter(
                    previousStoreVersion,
                    nextStoreVersion,
                    topicName,
                    partitionCount,
                    generationId,
                    dataCenterName,
                    brokerAddress,
                    deadlineNs),
                regionExecutor));
      }

      CompletableFuture.allOf(dataCenterBroadcastFutureMap.values().toArray(new CompletableFuture[0]))
          .get(getRemainingTimeInMs(deadlineNs), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException(
          getBroadcastFailureMessage(
              generationId,
              storeName,
              previousStoreVersion,
              nextStoreVersion,
              topicName,
              dataCenterBroadcastFutureMap),
          e);
    } catch (ExecutionException | TimeoutException e) {
      throw new VeniceException(
          getBroadcastFailureMessage(
              generationId,
              storeName,
              previousStoreVersion,
              nextStoreVersion,
              topicName,
              dataCenterBroadcastFutureMap),
          e);
    } finally {
      regionExecutor.shutdownNow();
    }
  }

  private void broadcastVersionSwapToDataCenter(
      Version previousStoreVersion,
      Version nextStoreVersion,
      String topicName,
      int partitionCount,
      long generationId,
      String dataCenterName,
      String brokerAddress,
      long deadlineNs) {
    VeniceWriter veniceWriter = null;
    boolean gracefulCloseCompleted = false;
    try {
      VeniceWriterOptions.Builder writerOptionsBuilder =
          new VeniceWriterOptions.Builder(topicName).setTime(getTimer()).setPartitionCount(partitionCount);
      if (!dataCenterName.equals(localDataCenterName)) {
        writerOptionsBuilder.setBrokerAddress(brokerAddress);
      }
      veniceWriter = veniceWriterFactory.createVeniceWriter(writerOptionsBuilder.build());
      List<CompletableFuture<PubSubProduceResult>> partitionFutures =
          veniceWriter.nonBlockingBroadcastVersionSwapWithRegionInfo(
              previousStoreVersion.kafkaTopicName(),
              nextStoreVersion.kafkaTopicName(),
              localDataCenterName,
              dataCenterName,
              generationId,
              Collections.emptyMap());
      CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
          .get(getRemainingTimeInMs(deadlineNs), TimeUnit.MILLISECONDS);
      veniceWriter.closeAsync(true).get(getRemainingTimeInMs(deadlineNs), TimeUnit.MILLISECONDS);
      gracefulCloseCompleted = true;
      LOGGER.info(
          "Successfully sent Version Swap message with generation id: {}, source data center: {} for store: {} from version: {} to version: {} to topic: {} in data center: {}",
          generationId,
          localDataCenterName,
          previousStoreVersion.getStoreName(),
          previousStoreVersion.getNumber(),
          nextStoreVersion.getNumber(),
          topicName,
          dataCenterName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new VeniceException("Interrupted while broadcasting Version Swap message to " + dataCenterName, e);
    } catch (ExecutionException | TimeoutException e) {
      throw new VeniceException("Failed to broadcast Version Swap message to " + dataCenterName, e);
    } finally {
      if (veniceWriter != null && !gracefulCloseCompleted) {
        veniceWriter.closeAsync(false);
      }
    }
  }

  private long getRemainingTimeInMs(long deadlineNs) throws TimeoutException {
    long remainingTimeInMs = TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime());
    if (remainingTimeInMs <= 0) {
      throw new TimeoutException("Version Swap broadcast exceeded its deadline");
    }
    return remainingTimeInMs;
  }

  private String getBroadcastFailureMessage(
      long generationId,
      String storeName,
      Version previousStoreVersion,
      Version nextStoreVersion,
      String topicName,
      Map<String, CompletableFuture<Void>> dataCenterBroadcastFutureMap) {
    StringBuilder incompleteDataCenters = new StringBuilder();
    for (Map.Entry<String, CompletableFuture<Void>> entry: dataCenterBroadcastFutureMap.entrySet()) {
      if (!entry.getValue().isDone() || entry.getValue().isCompletedExceptionally()) {
        if (incompleteDataCenters.length() > 0) {
          incompleteDataCenters.append(',');
        }
        incompleteDataCenters.append(entry.getKey());
      }
    }
    String message = String.format(
        "Failed to broadcast Version Swap message with generation id: %s, source data center: %s for store: %s from version: %s to version: %s to topic: %s in data center(s): %s",
        generationId,
        localDataCenterName,
        storeName,
        previousStoreVersion.getNumber(),
        nextStoreVersion.getNumber(),
        topicName,
        incompleteDataCenters);
    LOGGER.error(message);
    return message;
  }

  long getVersionSwapGenerationId() {
    return getTimer().getMilliseconds();
  }
}
