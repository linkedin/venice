package com.linkedin.venice.stats;


import com.linkedin.venice.kafka.consumer.StoreConsumptionTask;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPerStoreService;
import com.linkedin.venice.meta.Version;

import java.util.Comparator;
import java.util.List;

public class OffsetLagStat extends LambdaStat {

  public OffsetLagStat(KafkaConsumerPerStoreService kafkaConsumerPerStoreService, String storeName) {
    super(() -> {
      List<StoreConsumptionTask> tasks =
        kafkaConsumerPerStoreService.getRunningConsumptionTasksByStore(storeName);
      if (tasks.isEmpty()) {
        return 0d;
      }

      return tasks.stream()
        .max(Comparator.comparing(task -> Version.parseVersionFromKafkaTopicName(task.getTopic())))
        .map(task -> (double) task.getOffsetLag())
        .get();
    });
  }
}
