package com.linkedin.venice.ingestion.control;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MultiRegionRealTimeTopicSwitcher extends RealTimeTopicSwitcher {
  private static final Logger LOGGER = LogManager.getLogger(MultiRegionRealTimeTopicSwitcher.class);
  private final Map<String, VeniceWriterFactory> allDataCenterWriterFactoryMap;
  private final String localDataCenterName;

  public MultiRegionRealTimeTopicSwitcher(
      TopicManager topicManager,
      VeniceWriterFactory localVeniceWriterFactory,
      VeniceProperties veniceProperties,
      PubSubTopicRepository pubSubTopicRepository,
      Map<String, VeniceWriterFactory> remoteDataCenterWriterFactoryMap,
      String localDataCenterName) {
    super(topicManager, localVeniceWriterFactory, veniceProperties, pubSubTopicRepository);
    this.allDataCenterWriterFactoryMap = new HashMap<>(remoteDataCenterWriterFactoryMap);
    this.allDataCenterWriterFactoryMap.put(localDataCenterName, localVeniceWriterFactory);
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
        "Broadcasting Version Swap message with generation id: {}, source data center: {} to topic: {} for store: {} to {} partitions and {} data center",
        generationId,
        localDataCenterName,
        topicName,
        storeName,
        partitionCount,
        allDataCenterWriterFactoryMap.size());
    for (Map.Entry<String, VeniceWriterFactory> entry: allDataCenterWriterFactoryMap.entrySet()) {
      String dataCenterName = entry.getKey();
      VeniceWriterFactory writerFactory = entry.getValue();
      try (VeniceWriter veniceWriter = writerFactory.createVeniceWriter(
          new VeniceWriterOptions.Builder(topicName).setTime(getTimer()).setPartitionCount(partitionCount).build())) {
        veniceWriter.broadcastVersionSwapWithRegionInfo(
            previousStoreVersion.kafkaTopicName(),
            nextStoreVersion.kafkaTopicName(),
            localDataCenterName,
            dataCenterName,
            generationId,
            Collections.emptyMap());
      }
      LOGGER.info(
          "Successfully sent Version Swap message with generation id: {}, source data center: {} for store: {} from version: {} to version: {} to topic: {} in data center: {}",
          generationId,
          localDataCenterName,
          storeName,
          previousStoreVersion.getNumber(),
          nextStoreVersion.getNumber(),
          topicName,
          dataCenterName);
    }
  }

  long getVersionSwapGenerationId() {
    return getTimer().getMilliseconds();
  }
}
