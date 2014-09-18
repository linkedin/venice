package kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.utils.VerifiableProperties;
import message.VeniceMessageSerializer;
import message.VeniceMessage;
import storage.InMemoryStoreNode;
import storage.VeniceStoreManager;
import storage.VeniceStoreNode;
import venice.VeniceClient;

/**
 * Created by clfung on 9/15/14.
 */
public class ConsumerTask implements Runnable {

  private KafkaStream m_stream;
  private int m_threadNumber;

  public ConsumerTask(KafkaStream a_stream, int a_threadNumber) {
    m_threadNumber = a_threadNumber;
    m_stream = a_stream;
  }

  public void run() {

    ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
    VeniceMessageSerializer messageSerializer = new VeniceMessageSerializer(new VerifiableProperties());
    VeniceMessage vm = null;
    VeniceStoreManager manager = VeniceStoreManager.getInstance();

    while (it.hasNext()) {
      vm = messageSerializer.toMessage(it.next().message());
      manager.storeValue(VeniceClient.TEST_KEY, vm);
      System.out.println("Consumed: " + vm.getPayload());
    }

    System.out.println("Shutting down Thread: " + m_threadNumber);

  }

}
