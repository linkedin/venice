package venice;

import config.GlobalConfiguration;
import kafka.consumer.HighKafkaConsumer;
import kafka.producer.KafkaProducer;
import kafka.consumer.SimpleKafkaConsumer;
import message.OperationType;
import message.VeniceMessage;
import org.apache.log4j.Logger;
import storage.InMemoryStoreNode;
import storage.VeniceStoreManager;
import storage.VeniceStoreNode;

import java.util.Arrays;
import java.util.Scanner;

/**
 * Created by clfung on 9/17/14.
 */
public class VeniceClient {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceClient.class.getName());

  private static final int NUM_THREADS = 1;

  private static final boolean USE_HIGH_CONSUMER = false;
  private static final int DEFAULT_PORT = 9092;

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

    // TODO: remove this once partitioning is established
    // add a dummy node to the store manager
    InMemoryStoreNode node = new InMemoryStoreNode(1);
    storeManager.registerNode(node);

    try {

      // optional use of high level consumer or simple consumer
      if (VeniceClient.USE_HIGH_CONSUMER) {

        HighKafkaConsumer highConsumer = new HighKafkaConsumer(cfg.getZookeeperURL(),
            "sample_group", VeniceClient.TEST_TOPIC);
        highConsumer.run(1);

      } else {

        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(TEST_TOPIC);

        // TODO: everything in partition 0 for now
        consumer.run(NUM_THREADS, 0, Arrays.asList("localhost"), DEFAULT_PORT);

      }

    } catch (Exception e) {
      logger.error("Consumer failure: " + e);
      e.printStackTrace();
    }

  }

  /**
   * A function used to mock client inputs into the Venice Server
   * */
  public void getInput() {

    // mocked input test
    KafkaProducer kp = new KafkaProducer();
    VeniceMessage msg;
    Scanner reader = new Scanner(System.in);

    while (true) {

      logger.info("Test Venice: ");
      String input = reader.nextLine();

      String[] commandArgs = input.split(" ");

      if (commandArgs[0].equals("put")) {

        msg = new VeniceMessage(OperationType.PUT, commandArgs[1]);
        kp.sendMessage(VeniceClient.TEST_KEY, msg);

        logger.info("Run a put: " + commandArgs[1]);

      } else if (commandArgs[0].equals(("get"))) {

        logger.info("Got: " + storeManager.getValue(VeniceClient.TEST_KEY));

      }

    }
  }

}
