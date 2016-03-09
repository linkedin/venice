package com.linkedin.venice.controller.kafka;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by mwise on 2/9/16.
 */
public class TopicCreatorTest {

  @Test
  public void shouldCreateATopic(){
    String zkConnection = "localhost:2181";
    int sessionTimeoutMs = 10 * 1000;
    int connectionTimeoutMs = 8 * 1000;

    TopicCreator creator = new TopicCreator(zkConnection, sessionTimeoutMs, connectionTimeoutMs);

//    creator.createTopic("test1", 2, 1);


  }
}
