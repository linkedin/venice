package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.memory.ClassSizeEstimator.getClassOverhead;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.memory.InstanceSizeEstimator;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.collections.MemoryBoundBlockingQueue;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class will spin up multiple producer instances to speed up the producing.
 * 1. Maintain a buffer queue per producer.
 * 2. The writes to the same partition will be routed to the same queue all the time to guarantee ordering.
 * 3. PubSubProducer in each thread will constantly be polling the corresponding queue and write to broker.
 *
 * Here are the main reasons we would like to leverage this new strategy in streaming processing job:
 * 1. In the batch push job, each reducer only produces to one single Venice partition, so the multiple-thread-multiple-producer
 *    wouldn't help much.
 * 2. In Venice-Server leader replica, it is natural to have multiple consumers (threads) producing to different
 *    Venice partitions as it is using partition-wise shared consumer assignment strategy, and multiple producers
 *    can help, and it will be covered by {@link PubSubProducerAdapterDelegator}.
 * 3. Online writer is sharing a similar pattern as #2, but so far, we haven't heard any complains regarding online
 *    writer performance, so we will leave it out of scope for now.
 * 4. Streaming job is typically running inside container and single-threaded to guarantee ordering, but the writes
 *    to Venice store can belong to several Venice partitions. We observed that KafkaProducer compression
 *    would introduce a lot of overheads for large records, which will slow down Producer#send and if Producer#send is slow,
 *    the streaming job throughput will be heavily affected.
 *    This strategy considers the following aspects:
 *    a. We still want to keep compression on producer end as the streaming app has enough cpu resources.
 *    b. We would like to utilize more cpu resources to improve the producing throughput.
 *    c. We would like to get round of the contention issue (KafkaProducer has several synchronized sections for sending
 *       messages to broker) when running in a multiple-threaded env.
 *
 *    Here is how this strategy would fulfill #4.
 *    1. Execute the Producer#send logic in a multi-thread env to better utilize cpu resources.
 *    2. Each thread will have its own {@link PubSubProducerAdapter} to avoid contention.
 *    3. The mapping between Venice partition and buffered producer is sticky, so the ordering is still guaranteed per
 *       Venice partition.
 *
 *
 * The above analysis is correct in the high-level, but there are still some nuances, where this new strategy can perform
 * better than the default one in VPJ reducer and Venice Server leader replicas:
 * 1. If the record processing time is comparable with the producer#send latency, the new strategy will delegate the producing
 *    logic to a separate thread, which can potentially improve the overall throughput if there are enough cpu resources.
 * 2. If the record processing time is minimal comparing with the producer#send latency, this new strategy won't help much.
 *
 * We will explore ^ after validating this new strategy in stream processing app.
 *
 */
public class PubSubProducerAdapterConcurrentDelegator implements PubSubProducerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(PubSubProducerAdapterConcurrentDelegator.class);
  private final String producingTopic;
  private final ExecutorService producerExecutor;
  private final List<MemoryBoundBlockingQueue<ProducerQueueNode>> blockingQueueList;
  private final Map<MemoryBoundBlockingQueue<ProducerQueueNode>, ProducerQueueNode> lastNodePerQueue;
  private final AtomicInteger selectedQueueId = new AtomicInteger(0);
  private final Map<Integer, MemoryBoundBlockingQueue<ProducerQueueNode>> partitionQueueAssignment;
  private final List<PubSubProducerAdapter> producers;
  private final List<ProducerQueueDrainer> drainers;

  public PubSubProducerAdapterConcurrentDelegator(
      String producingTopic,
      int producerThreadCount,
      int producerQueueSize,
      Supplier<PubSubProducerAdapter> producerAdapterSupplier) {
    if (producerThreadCount <= 1) {
      throw new VeniceException("'producerThreadCount' should be larger than 1, but got " + producerThreadCount);
    }
    if (producerQueueSize <= 2 * 1024 * 1024) {
      throw new VeniceException("'producerQueueSize' should be larger than 2MB, but got " + producerQueueSize);
    }
    LOGGER.info(
        "Initializing a 'PubSubProducerAdapterConcurrentDelegator' instance for topic: {} with thread"
            + " count: {} and queue size: {}",
        producingTopic,
        producerThreadCount,
        producerQueueSize);
    List<MemoryBoundBlockingQueue<ProducerQueueNode>> tmpBlockingQueueList = new ArrayList<>(producerThreadCount);
    this.producerExecutor = Executors
        .newFixedThreadPool(producerThreadCount, new DaemonThreadFactory(producingTopic + "-concurrent-producer"));
    this.partitionQueueAssignment = new VeniceConcurrentHashMap<>();
    this.lastNodePerQueue = new VeniceConcurrentHashMap<>(producerThreadCount);
    this.producingTopic = producingTopic;
    List<PubSubProducerAdapter> tmpProducers = new ArrayList<>(producerThreadCount);
    List<ProducerQueueDrainer> tmpDrainers = new ArrayList<>(producerThreadCount);
    for (int cur = 0; cur < producerThreadCount; ++cur) {
      PubSubProducerAdapter producer = producerAdapterSupplier.get();
      tmpProducers.add(producer);

      MemoryBoundBlockingQueue<ProducerQueueNode> blockingQueue =
          new MemoryBoundBlockingQueue<>(producerQueueSize, 1 * 1024 * 1024l);
      ProducerQueueDrainer drainer = new ProducerQueueDrainer(producingTopic, blockingQueue, cur, producer);
      tmpBlockingQueueList.add(blockingQueue);
      tmpDrainers.add(drainer);
      this.producerExecutor.submit(drainer);
    }

    this.blockingQueueList = Collections.unmodifiableList(tmpBlockingQueueList);
    this.producers = Collections.unmodifiableList(tmpProducers);
    this.drainers = Collections.unmodifiableList(tmpDrainers);
  }

  public static class ProducerQueueNode implements Measurable {
    /** We assume that the {@link #produceFuture} is empty, and the {@link #topic} is a shared instance. */
    private static final int PRODUCER_QUEUE_NODE_PARTIAL_OVERHEAD =
        getClassOverhead(ProducerQueueNode.class) + getClassOverhead(CompletableFuture.class);

    private final String topic;
    private final int partition;
    private final KafkaKey key;
    private final KafkaMessageEnvelope value;
    private final PubSubMessageHeaders headers;
    private final PubSubProducerCallback callback;
    private final CompletableFuture<PubSubProduceResult> produceFuture;

    public ProducerQueueNode(
        String topic,
        int partition,
        KafkaKey key,
        KafkaMessageEnvelope value,
        PubSubMessageHeaders headers,
        PubSubProducerCallback callback,
        CompletableFuture<PubSubProduceResult> produceFuture) {
      this.topic = topic;
      this.partition = partition;
      this.key = key;
      this.value = value;
      this.headers = headers;
      this.callback = callback;
      this.produceFuture = produceFuture;
    }

    @Override
    public int getHeapSize() {
      int size = PRODUCER_QUEUE_NODE_PARTIAL_OVERHEAD + key.getHeapSize() + InstanceSizeEstimator.getSize(value)
          + headers.getHeapSize();
      if (this.callback instanceof Measurable) {
        size += ((Measurable) this.callback).getHeapSize();
      }
      return size;
    }
  }

  public static class ProducerQueueDrainer implements Runnable {
    private static final Logger LOGGER = LogManager.getLogger(ProducerQueueDrainer.class);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final String producingTopic;
    private final BlockingQueue<ProducerQueueNode> blockingQueue;
    private final int drainerIndex;
    private final PubSubProducerAdapter producer;

    public ProducerQueueDrainer(
        String producingTopic,
        BlockingQueue<ProducerQueueNode> blockingQueue,
        int drainerIndex,
        PubSubProducerAdapter producer) {
      this.producingTopic = producingTopic;
      this.blockingQueue = blockingQueue;
      this.drainerIndex = drainerIndex;
      this.producer = producer;
    }

    @Override
    public void run() {
      LOGGER
          .info("Starting ProducerQueueDrainer Thread for drainer: {} to topic: {} ...", drainerIndex, producingTopic);
      while (isRunning.get()) {
        try {
          final ProducerQueueNode node = blockingQueue.take();
          try {
            producer.sendMessage(node.topic, node.partition, node.key, node.value, node.headers, node.callback)
                .whenComplete((result, t) -> {
                  if (t != null) {
                    node.produceFuture.completeExceptionally(t);
                  } else {
                    node.produceFuture.complete(result);
                  }
                });
          } catch (Exception ex) {
            LOGGER.error(
                "Got exception when producing record to topic: {}, partition: {} in drainer: {}",
                node.topic,
                node.partition,
                drainerIndex,
                ex);
            // TODO: do we need to add retry here?
            throw ex;
          }
        } catch (InterruptedException e) {
          LOGGER.warn(
              "Got interrupted when executing producer drainer to topic: {} with index: {}",
              producingTopic,
              drainerIndex);
        }
      }
      isRunning.set(false);
      LOGGER.info(
          "Current ProducerQueueDrainer Thread for drainer: {} to topic: {} stopped",
          drainerIndex,
          producingTopic);
    }

    public void stop() {
      isRunning.set(false);
    }

  }

  @Override
  public int getNumberOfPartitions(String topic) {
    return producers.get(0).getNumberOfPartitions(topic);
  }

  public boolean hasAnyProducerStopped() {
    for (ProducerQueueDrainer drainer: drainers) {
      if (!drainer.isRunning.get()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public CompletableFuture<PubSubProduceResult> sendMessage(
      String topic,
      Integer partition,
      KafkaKey key,
      KafkaMessageEnvelope value,
      PubSubMessageHeaders pubSubMessageHeaders,
      PubSubProducerCallback pubSubProducerCallback) {
    if (hasAnyProducerStopped()) {
      throw new VeniceException("Some internal producer has already stopped running, pop-up the exception");
    }
    /**
     * Round-robin assignment.
     */
    MemoryBoundBlockingQueue<ProducerQueueNode> selectedQueue = partitionQueueAssignment.computeIfAbsent(
        partition,
        ignored -> blockingQueueList.get(selectedQueueId.getAndIncrement() % blockingQueueList.size()));
    CompletableFuture<PubSubProduceResult> resultFuture = new CompletableFuture<>();
    try {
      ProducerQueueNode newNode = new ProducerQueueNode(
          topic,
          partition,
          key,
          value,
          pubSubMessageHeaders,
          pubSubProducerCallback,
          resultFuture);
      selectedQueue.put(newNode);
      lastNodePerQueue.put(selectedQueue, newNode);
    } catch (InterruptedException e) {
      throw new VeniceException("Failed to produce to topic: " + topic, e);
    }

    return resultFuture;
  }

  @Override
  public void flush() {
    LOGGER.info("Start flushing a 'PubSubProducerAdapterConcurrentDelegator' instance for topic: {}", producingTopic);
    /**
     * Take the last message of each queue and make sure they will be produced before the producer flush.
     */
    try {
      for (ProducerQueueNode node: lastNodePerQueue.values()) {
        node.produceFuture.get();
      }
    } catch (Exception e) {
      throw new VeniceException("Received exception when trying to flush the pending messages", e);
    }

    producers.forEach(p -> p.flush());
    LOGGER
        .info("Finished flushing a 'PubSubProducerAdapterConcurrentDelegator' instance for topic: {}", producingTopic);
  }

  @Override
  public void close(long closeTimeOutMs) {
    LOGGER.info("Start closing a 'PubSubProducerAdapterConcurrentDelegator' instance for topic: {}", producingTopic);
    drainers.forEach(d -> d.stop());
    producerExecutor.shutdownNow();
    try {
      producerExecutor.awaitTermination(closeTimeOutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOGGER.error(
          "Received InterruptedException while shutting down the producer executor, will continue"
              + " to close the producers",
          e);
    }

    producers.forEach(p -> p.close(closeTimeOutMs));
    LOGGER.info("Closed a 'PubSubProducerAdapterConcurrentDelegator' instance for topic: {}", producingTopic);
  }

  @Override
  public Object2DoubleMap<String> getMeasurableProducerMetrics() {
    return Object2DoubleMaps.emptyMap();
  }

  @Override
  public String getBrokerAddress() {
    return producers.get(0).getBrokerAddress();
  }
}
