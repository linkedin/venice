package kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import kafka.consumer.Consumer;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by clfung on 9/12/14.
 */
public class KafkaConsumer {

  private final ConsumerConnector zkConnector;
  private final String topic;
  private ExecutorService executor;

  public KafkaConsumer(String zookeeper, String groupId, String topic) {

    zkConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
    this.topic = topic;

  }

  private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
    Properties props = new Properties();

    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);

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
      executor.submit(new ConsumerTask(stream, threadNumber));
      threadNumber++;
    }

  }

}
