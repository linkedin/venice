package com.linkedin.venice.kafka.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.VerifiableProperties;
import com.linkedin.venice.message.VeniceMessageSerializer;
import com.linkedin.venice.message.VeniceMessage;
import org.apache.log4j.Logger;
import com.linkedin.venice.storage.VeniceStorageManager;


/**
 * Runnable class which performs Kafka consumption from the High Level Consumer API.
 */
public class HighConsumerTask implements Runnable {

  static final Logger logger = Logger.getLogger(HighConsumerTask.class.getName());

  private KafkaStream stream;
  private int threadNumber; // thread number for this process

  public HighConsumerTask(KafkaStream stream, int threadNumber) {
    this.threadNumber = threadNumber;
    this.stream = stream;
  }

  /**
   *  Parallelized method which performs Kafka consumption
   * */
  public void run() {

    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    VeniceMessageSerializer messageSerializer = new VeniceMessageSerializer(new VerifiableProperties());
    VeniceMessage vm = null;
    VeniceStorageManager manager = VeniceStorageManager.getInstance();

    while (it.hasNext()) {
      vm = messageSerializer.fromBytes(it.next().message());
      manager.storeValue(1, new String(it.next().key()), vm);
      logger.info("Consumed: " + vm.getPayload());
    }

    logger.warn("Shutting down Thread: " + threadNumber);

  }

}
