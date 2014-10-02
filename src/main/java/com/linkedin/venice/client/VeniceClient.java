package com.linkedin.venice.client;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.producer.KafkaProducer;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.server.VeniceServer;
import org.apache.log4j.Logger;

/**
 * Class which acts as the primary client API
 * Created by clfung on 9/17/14.
 */
public class VeniceClient {

  // log4j logger
  static final Logger logger = Logger.getLogger(VeniceClient.class.getName());

  private VeniceServer server;
  private KafkaProducer kp;

  private VeniceMessage msg;

  public VeniceClient() {

    // TODO: implement file input for config
    GlobalConfiguration.initialize("");

    server = new VeniceServer();
    kp = new KafkaProducer();

  }

  /**
   * Execute a standard "get" on the key. Returns null if empty.
   * @param key - The key to look for in storage.
   * @return The result of the "Get" operation
   * */
  public Object get(String key) {
    return server.readValue(key);
  }

  /**
   * Execute a standard "delete" on the key.
   * @param key - The key to delete in storage.
   * */
  public void delete(String key) {

    msg = new VeniceMessage(OperationType.DELETE, "");
    kp.sendMessage(key, msg);

  }

  /**
   * Execute a standard "put" on the key.
   * @param key - The key to put in storage.
   * @param value - The value to be associated with the given key
   * */
  public void put(String key, Object value) {

    msg = new VeniceMessage(OperationType.PUT, value.toString());
    kp.sendMessage(key, msg);

  }

}
