package com.linkedin.venice.stats;


import com.linkedin.venice.kafka.consumer.StoreConsumptionTask;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPerStoreService;
import org.apache.log4j.Logger;

import java.util.List;

public class OffsetLagStat extends LambdaStat {
  private final static Logger logger = Logger.getLogger(OffsetLagStat.class);

  public OffsetLagStat(KafkaConsumerPerStoreService kafkaConsumerPerStoreService, String storeName) {
    super(() -> {
      List<StoreConsumptionTask> tasks =
        kafkaConsumerPerStoreService.getRunningConsumptionTasksByStore(storeName);
      if (tasks.isEmpty()) {
        return 0d;
      }

      if (tasks.size() > 1) {
        logger.warn("More than one tasks are running for store: " + storeName
          + ". You are doing parallel push. Pick up the max offsetLag among tasks");
      }

      return (double) tasks.stream()
        .map(StoreConsumptionTask::getOffsetLag)
        .reduce(0l, (lag1, lag2) -> Long.max(lag1, lag2));
    });
  }
}
