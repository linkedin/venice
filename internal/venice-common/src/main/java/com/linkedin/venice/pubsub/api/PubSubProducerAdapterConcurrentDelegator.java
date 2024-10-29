package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.common.Measurable;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.ProtocolUtils;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.collections.MemoryBoundBlockingQueue;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import java.util.ArrayList;
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
 * This pattern is mainly useful for stream processing as one container can write to multiple partitions.
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
      throw new VeniceException("'producerThreadCount' should be larger than 1");
    }
    if (producerQueueSize <= 2 * 1024 * 1024) {
      throw new VeniceException("'producerQueueSize' should be larger than 2MB");
    }
    LOGGER.info(
        "Initializing a 'PubSubProducerAdapterConcurrentDelegator' instance for topic: {} with thread"
            + " count: {} and queue size: {}",
        producingTopic,
        producerThreadCount,
        producerQueueSize);
    this.blockingQueueList = new ArrayList<>(producerThreadCount);
    this.producerExecutor = Executors
        .newFixedThreadPool(producerThreadCount, new DaemonThreadFactory(producingTopic + "-concurrent-producer"));
    this.partitionQueueAssignment = new VeniceConcurrentHashMap<>();
    this.lastNodePerQueue = new VeniceConcurrentHashMap<>(producerThreadCount);
    this.producingTopic = producingTopic;
    this.producers = new ArrayList<>(producerThreadCount);
    this.drainers = new ArrayList<>(producerThreadCount);
    for (int cur = 0; cur < producerThreadCount; ++cur) {
      PubSubProducerAdapter producer = producerAdapterSupplier.get();
      this.producers.add(producer);

      MemoryBoundBlockingQueue<ProducerQueueNode> blockingQueue =
          new MemoryBoundBlockingQueue<>(producerQueueSize, 1 * 1024 * 1024l);
      ProducerQueueDrainer drainer = new ProducerQueueDrainer(blockingQueue, cur, producer);
      this.blockingQueueList.add(blockingQueue);
      this.drainers.add(drainer);
      this.producerExecutor.submit(drainer);
    }
  }

  public static class ProducerQueueNode implements Measurable {
    /**
     * Considering the overhead of {@link PubSubMessage} and its internal structures.
     */
    private static final int QUEUE_NODE_OVERHEAD_IN_BYTE = 256;
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
    public int getSize() {
      int headerSize = 0;
      if (headers != null) {
        for (PubSubMessageHeader header: headers.toList()) {
          headerSize += header.key().length() + header.value().length;
        }
      }
      return topic.length() + key.getEstimatedObjectSizeOnHeap()
          + ProtocolUtils.getEstimateOfMessageEnvelopeSizeOnHeap(value) + headerSize + QUEUE_NODE_OVERHEAD_IN_BYTE;
    }
  }

  public static class ProducerQueueDrainer implements Runnable {
    private final Logger LOGGER = LogManager.getLogger(ProducerQueueDrainer.class);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final BlockingQueue<ProducerQueueNode> blockingQueue;
    private final int drainerIndex;
    private final PubSubProducerAdapter producer;

    public ProducerQueueDrainer(
        BlockingQueue<ProducerQueueNode> blockingQueue,
        int drainerIndex,
        PubSubProducerAdapter producer) {
      this.blockingQueue = blockingQueue;
      this.drainerIndex = drainerIndex;
      this.producer = producer;
    }

    @Override
    public void run() {
      LOGGER.info("Starting ProducerQueueDrainer Thread for drainer: {}...", drainerIndex);
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
          LOGGER.warn("Got interrupted when executing producer drainer with index: {}", drainerIndex);
        }
      }
      isRunning.set(false);
      LOGGER.info("Current ProducerQueueDrainer Thread for drainer: {} stopped", drainerIndex);
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
