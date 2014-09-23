package kafka.consumer;

import kafka.utils.VerifiableProperties;
import message.VeniceMessageSerializer;
import message.VeniceMessage;
import org.apache.log4j.Logger;
import storage.VeniceStoreManager;
import venice.VeniceClient;


/**
 * Created by clfung on 9/15/14.
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
    VeniceStoreManager manager = VeniceStoreManager.getInstance();

    while (it.hasNext()) {
      vm = messageSerializer.fromBytes(it.next().message());
      manager.storeValue(VeniceClient.TEST_KEY, vm);
      logger.info("Consumed: " + vm.getPayload());
    }

    logger.warn("Shutting down Thread: " + threadNumber);

  }

}
