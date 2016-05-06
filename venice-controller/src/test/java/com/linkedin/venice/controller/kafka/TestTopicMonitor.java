package com.linkedin.venice.controller.kafka;

import org.testng.annotations.Test;


/**
 * Created by mwise on 5/6/16.
 */
public class TestTopicMonitor {
  @Test
  public void topicMonitorStartsAndStops() throws Exception {
    TopicMonitor mon = new TopicMonitor();
    mon.start();

    Thread.sleep(10000);
    mon.stop();
  }
}
