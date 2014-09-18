package venice;

import config.GlobalConfiguration;
import kafka.KafkaConsumer;
import kafka.KafkaProducer;
import message.OperationType;
import message.VeniceMessage;
import metadata.KeyCache;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import storage.InMemoryStoreNode;
import storage.VeniceStoreManager;
import storage.VeniceStoreNode;

import java.util.Scanner;

/**
 * Created by clfung on 9/17/14.
 */
public class VeniceClient {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceStoreNode.class.getName());

  public static final int NUM_THREADS = 1;

  public static final String TEST_TOPIC = "test_topic";
  public static final String TEST_KEY = "test_key";

  private GlobalConfiguration cfg;
  private VeniceStoreManager storeManager;

  public VeniceClient() {

    // get configuration from hardcoded constants
    cfg = new GlobalConfiguration();

    // TODO: implement file input for config
    cfg.initialize("");

    // initialize the storage engine
    storeManager = VeniceStoreManager.getInstance();

    InMemoryStoreNode node = new InMemoryStoreNode(1);

    storeManager.registerNode(node);

    // start the consumer
    KafkaConsumer consumer = new KafkaConsumer(cfg.getZookeeperURL(), "group1", VeniceClient.TEST_TOPIC);
    consumer.run(NUM_THREADS);

  }

  public void getInput() {

    // mocked input test
    KafkaProducer kp = new KafkaProducer();
    VeniceMessage msg;
    Scanner reader = new Scanner(System.in);

    while (true) {
      System.out.println("Test Venice: ");
      String input = reader.nextLine();

      String[] commandArgs = input.split(" ");

      if (commandArgs[0].equals("put")) {

        msg = new VeniceMessage(OperationType.PUT, commandArgs[1]);
        kp.sendMessage(msg);

        System.out.println("Run a put: " + commandArgs[1]);

      } else if (commandArgs[0].equals(("get"))) {

        System.out.println(storeManager.getValue(VeniceClient.TEST_KEY));

      }

    }
  }

}
