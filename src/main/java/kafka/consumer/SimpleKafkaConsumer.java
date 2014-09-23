package kafka.consumer;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by clfung on 9/18/14.
 */
public class SimpleKafkaConsumer {

  static final Logger logger = Logger.getLogger(SimpleKafkaConsumer.class.getName());

  public static final int MAX_READS = 100000000;

  private ExecutorService executor;
  private String topic;

  public SimpleKafkaConsumer(String topic) {

    this.topic = topic;

  }

  public void run(int numThreads, int partition, List<String> brokers, int port) {

    // launch all the threads
    executor = Executors.newFixedThreadPool(numThreads);

    // create an object to consume the messages
    for (int i = 0; i < numThreads; i++) {
      executor.submit(new SimpleKafkaConsumerTask(MAX_READS, topic, partition, brokers, port, i));
    }

  }

  public void shutdown() {

    logger.error("Shutting down consumer service...");

    if (executor != null) {
      executor.shutdown();
    }

  }


}
