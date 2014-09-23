package com.linkedin.venice.client;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.HighKafkaConsumer;
import com.linkedin.venice.kafka.producer.KafkaProducer;
import com.linkedin.venice.kafka.consumer.SimpleKafkaConsumer;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.server.VeniceServer;
import org.apache.log4j.Logger;
import com.linkedin.venice.storage.InMemoryStoreNode;
import com.linkedin.venice.storage.VeniceStoreManager;

import java.util.Arrays;
import java.util.Scanner;

/**
 * Created by clfung on 9/17/14.
 */
public class VeniceClient {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceClient.class.getName());

  private VeniceServer server;

  public VeniceClient() {

    server = VeniceServer.getInstance();

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

      String input = reader.nextLine();

      String[] commandArgs = input.split(" ");

      if (commandArgs.length > 1) {

        if (commandArgs[0].equals("put")) {

          if (commandArgs.length > 2) {
            msg = new VeniceMessage(OperationType.PUT, commandArgs[2]);
            kp.sendMessage(commandArgs[1], msg);
          }

        } else if (commandArgs[0].equals(("get"))) {

          logger.info("Got: " + server.readValue(commandArgs[1]));

        } else if (commandArgs[0].equals("delete")) {

          msg = new VeniceMessage(OperationType.DELETE, "");
          kp.sendMessage(commandArgs[1], msg);

        }
      }

    }
  }

}
