package com.linkedin.venice.kafka.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by clfung on 9/12/14.
 */
public class HighKafkaConsumer {

  static final Logger logger = Logger.getLogger(HighKafkaConsumer.class.getName());

  private final ConsumerConnector zkConnector;
  private final String topic;
  private ExecutorService executor;

  public HighKafkaConsumer(String zookeeper, String groupId, String topic) {

    zkConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
    this.topic = topic;

  }

  /*
  * Provides configuration for the High Level Consumer to communicate with ZK.
  * */
  private static ConsumerConfig createConsumerConfig(String zookeeperURL, String consumerGroup) {

    Properties props = new Properties();

    props.put("zookeeper.connect", zookeeperURL);
    props.put("group.id", consumerGroup);

    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.offset.reset", "smallest");

    return new ConsumerConfig(props);

  }

  public void run(int numThreads) {

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(numThreads));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = zkConnector.createMessageStreams(topicCountMap);
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

    // launch all the threads
    executor = Executors.newFixedThreadPool(numThreads);

    // create an object to consume the messages
    int threadNumber = 0;
    for (final KafkaStream stream : streams) {
      executor.submit(new HighConsumerTask(stream, threadNumber));
      threadNumber++;
    }

  }

  public void close() {

    logger.error("Shutting down consumer service...");

    if (zkConnector != null) {
      zkConnector.shutdown();
    }

    if (executor != null) {
      executor.shutdown();
    }

  }

}
