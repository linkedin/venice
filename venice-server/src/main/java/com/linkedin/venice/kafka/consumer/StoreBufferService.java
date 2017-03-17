package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.DaemonThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class is serving as a {@link ConsumerRecord} buffer with an accompanying pool of drainer threads. The drainers
 * pull records out of the buffer and delegate the persistence and validation to the appropriate {@link StoreIngestionTask}.
 *
 * High-level idea:
 * 1. {@link StoreBufferService} will be maintaining a fixed number (configurable) of {@link StoreBufferDrainer} pool;
 * 2. For each {@link StoreBufferDrainer}, there is a corresponding {@link BlockingQueue}, which will buffer {@link QueueNode};
 * 3. All the records belonging to the same topic+partition will be allocated to the same drainer thread, otherwise DIV will fail;
 * 4. The logic to assign topic+partition to drainer, please check {@link #getDrainerIndexForConsumerRecord(ConsumerRecord)};
 * 5. There is still a thread executing {@link StoreIngestionTask} for each topic, which will handle admin actions, such
 * as subscribe, unsubscribe, kill and so on, and also poll consumer records from Kafka and put them into {@link #blockingQueueArr}
 * maintained by {@link StoreBufferService};
 *
 * For now, the assumption is that one-consumer-polling-thread should be fast enough to catch up with Kafka MM replication,
 * and data processing is the slowest part. If we find that polling is also slow later on, we may consider to adopt a consumer
 * thread pool to speed up polling from local Kafka brokers.
 */
public class StoreBufferService extends AbstractVeniceService {
  /**
   * Queue node type in {@link BlockingQueue} of each drainer thread.
   */
  private static class QueueNode implements Measurable {
    /**
     * Considering the overhead of {@link ConsumerRecord} and its internal structures.
     */
    private static final int QUEUE_NODE_OVERHEAD_IN_BYTE = 256;
    private ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord;
    private StoreIngestionTask consumptionTask;

    public QueueNode(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord, StoreIngestionTask consumptionTask) {
      this.consumerRecord = consumerRecord;
      this.consumptionTask = consumptionTask;
    }

    public ConsumerRecord<KafkaKey, KafkaMessageEnvelope> getConsumerRecord() {
      return this.consumerRecord;
    }

    public StoreIngestionTask getConsumptionTask() {
      return this.consumptionTask;
    }

    /**
     * This function is being used by {@link BlockingQueue#contains(Object)}.
     * The goal is to find out whether the buffered queue still has any records belonging to the specified topic+partition.
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null)
        return false;
      if (getClass() != o.getClass())
        return false;

      QueueNode node = (QueueNode)o;
      return this.consumerRecord.topic().equals(node.consumerRecord.topic()) &&
          this.consumerRecord.partition() == node.consumerRecord.partition();

    }

    @Override
    public int getSize() {
      return this.consumerRecord.serializedKeySize() +
          this.consumerRecord.serializedValueSize() +
          this.consumerRecord.topic().length() +
          QUEUE_NODE_OVERHEAD_IN_BYTE;
    }

    @Override
    public String toString() {
      return this.consumerRecord.toString();
    }
  };

  /**
   * Worker thread, which will invoke {@link StoreIngestionTask#processConsumerRecord(ConsumerRecord)} to process
   * each {@link ConsumerRecord} buffered in {@link BlockingQueue}.
   */
  private static class StoreBufferDrainer implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(StoreBufferDrainer.class);
    private final BlockingQueue<QueueNode> blockingQueue;

    public StoreBufferDrainer(BlockingQueue<QueueNode> blockingQueue) {
      this.blockingQueue = blockingQueue;
    }

    @Override
    public void run() {
      QueueNode node = null;
      while (true) {
        try {
          node = blockingQueue.take();
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException, will exit");
          break;
        }
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = node.getConsumerRecord();
        StoreIngestionTask consumptionTask = node.getConsumptionTask();

        try {
          consumptionTask.processConsumerRecord(consumerRecord);
        } catch (Exception e) {
          LOGGER.error("Got exception during processing consumer record: " + consumerRecord, e);
          /**
           * Catch all the thrown exception and store it in {@link StoreIngestionTask#lastWorkerException}.
           */
          consumptionTask.setLastDrainerException(e);
        }
      }
    }
  }

  private static final Logger LOGGER =  Logger.getLogger(StoreBufferService.class);

  private final int drainerNum;
  private final ArrayList<MemoryBoundBlockingQueue<QueueNode>> blockingQueueArr;
  private ExecutorService executorService;

  public StoreBufferService(int drainerNum, long bufferCapacityPerDrainer, long bufferNotifyDelta) {
    this.drainerNum = drainerNum;
    this.blockingQueueArr = new ArrayList<>();
    for (int cur = 0; cur < drainerNum; ++cur) {
      this.blockingQueueArr.add(new MemoryBoundBlockingQueue<>(bufferCapacityPerDrainer, bufferNotifyDelta));
    }
  }

  private String getTopicPartitionStr(String topic, int partition) {
    return topic + "-" + partition;
  }

  private int getDrainerIndexForConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    String topicPartition = getTopicPartitionStr(consumerRecord.topic(), consumerRecord.partition());
    return Math.abs(topicPartition.hashCode() % this.drainerNum);
  }

  public void putConsumerRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord,
                                StoreIngestionTask consumptionTask) throws InterruptedException {
    int drainerIndex = getDrainerIndexForConsumerRecord(consumerRecord);
    blockingQueueArr.get(drainerIndex)
        .put(new QueueNode(consumerRecord, consumptionTask));
  }

  /**
   * This function is used to drain all the records for the specified topic + partition.
   * The reason is that we don't want overlap Kafka messages between two different subscriptions,
   * which could introduce complicate dependencies in {@link StoreIngestionTask}.
   * @param topic
   * @param partition
   * @throws InterruptedException
   */
  public void drainBufferedRecordsFromTopicPartition(String topic, int partition) throws InterruptedException {
    int retryNum = 1000;
    int sleepIntervalInMS = 50;
    internalDrainBufferedRecordsFromTopicPartition(topic, partition, retryNum, sleepIntervalInMS);
  }

  protected void internalDrainBufferedRecordsFromTopicPartition(String topic, int partition,int retryNum,
                                                                int sleepIntervalInMS) throws InterruptedException {
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> fakeRecord = new ConsumerRecord<>(topic, partition, -1, null, null);
    int workerIndex = getDrainerIndexForConsumerRecord(fakeRecord);
    BlockingQueue<QueueNode> blockingQueue = blockingQueueArr.get(workerIndex);
    QueueNode fakeNode = new QueueNode(fakeRecord, null);

    int cur = 0;
    while (cur++ < retryNum) {
      if (!blockingQueue.contains(fakeNode)) {
        LOGGER.info("The blocking queue of store writer thread: " + workerIndex + " doesn't contain any record for topic: "
            + topic + " partition: " + partition);
        return;
      }
      Thread.sleep(sleepIntervalInMS);
    }
    String errorMessage = "There are still some records left in the blocking queue of store writer thread: " +
        workerIndex + " for topic: " + topic + " partition after retry for " + retryNum + " times";
    LOGGER.error(errorMessage);
    throw new VeniceException(errorMessage);
  }

  @Override
  public boolean startInner() throws Exception {
    this.executorService = Executors.newFixedThreadPool(drainerNum, new DaemonThreadFactory("Store-writer"));

    // Submit all the buffer drainers
    for (int cur = 0; cur < drainerNum; ++cur) {
      this.executorService.submit(new StoreBufferDrainer(this.blockingQueueArr.get(cur)));
    }
    this.executorService.shutdown();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    if (null != this.executorService) {
      this.executorService.shutdownNow();
      this.executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  public long getTotalMemoryUsage() {
    long totalUsage = 0;
    for (MemoryBoundBlockingQueue<QueueNode> queue : blockingQueueArr) {
      totalUsage += queue.getMemoryUsage();
    }

    return totalUsage;
  }

  public long getMaxMemoryUsagePerDrainer() {
    long maxUsage = 0;
    for (MemoryBoundBlockingQueue<QueueNode> queue : blockingQueueArr) {
      maxUsage = Math.max(maxUsage, queue.getMemoryUsage());
    }
    return maxUsage;
  }

  public long getMinMemoryUsagePerDrainer() {
    long minUsage = Long.MAX_VALUE;
    for (MemoryBoundBlockingQueue<QueueNode> queue : blockingQueueArr) {
      minUsage = Math.min(minUsage, queue.getMemoryUsage());
    }
    return minUsage;
  }
}
