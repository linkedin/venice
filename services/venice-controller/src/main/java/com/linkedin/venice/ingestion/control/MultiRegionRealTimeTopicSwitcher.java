package com.linkedin.venice.ingestion.control;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MultiRegionRealTimeTopicSwitcher extends RealTimeTopicSwitcher {
  private static final Logger LOGGER = LogManager.getLogger(MultiRegionRealTimeTopicSwitcher.class);
  private static final int DEFAULT_PER_DATA_CENTER_BROADCAST_TIMEOUT_IN_SECONDS = 60;
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
    Map<String, CompletableFuture<Void>> dataCenterBroadcastFutureMap = new HashMap<>();
    for (Map.Entry<String, String> entry: allAASourceDataCenterBrokerAddressMap.entrySet()) {
      String dataCenterName = entry.getKey();
      VeniceWriterOptions.Builder writerOptionsBuilder =
          new VeniceWriterOptions.Builder(topicName).setTime(getTimer()).setPartitionCount(partitionCount);
      if (!dataCenterName.equals(localDataCenterName)) {
        writerOptionsBuilder.setBrokerAddress(entry.getValue());
      }
      try (VeniceWriter veniceWriter = veniceWriterFactory.createVeniceWriter(writerOptionsBuilder.build())) {
        List<CompletableFuture<PubSubProduceResult>> futures =
            veniceWriter.nonBlockingBroadcastVersionSwapWithRegionInfo(
                previousStoreVersion.kafkaTopicName(),
                nextStoreVersion.kafkaTopicName(),
                localDataCenterName,
                dataCenterName,
                generationId,
                Collections.emptyMap());
        dataCenterBroadcastFutureMap
            .put(dataCenterName, CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])));
      }
    }

    // Check the result of each data center broadcast
    for (Map.Entry<String, CompletableFuture<Void>> entry: dataCenterBroadcastFutureMap.entrySet()) {
      try {
        entry.getValue().get(DEFAULT_PER_DATA_CENTER_BROADCAST_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        LOGGER.info(
            "Successfully sent Version Swap message with generation id: {}, source data center: {} for store: {} from version: {} to version: {} to topic: {} in data center: {}",
            generationId,
            localDataCenterName,
            storeName,
            previousStoreVersion.getNumber(),
            nextStoreVersion.getNumber(),
            topicName,
            entry.getValue());
      } catch (Exception e) {
        String message = String.format(
            "Failed to broadcast Version Swap message with generation id: %s, source data center: %s for store: %s from version: %s to version: %s to topic: %s in data center: %s",
            generationId,
            localDataCenterName,
            storeName,
            previousStoreVersion.getNumber(),
            nextStoreVersion.getNumber(),
            topicName,
            entry.getKey());
        LOGGER.error(message, e);
        throw new VeniceException(message);
      }
    }
  }

  long getVersionSwapGenerationId() {
    return getTimer().getMilliseconds();
  }
}
