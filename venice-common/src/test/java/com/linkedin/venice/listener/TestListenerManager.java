package com.linkedin.venice.listener;

import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.Map;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestListenerManager {
  private ListenerManager<RoutingDataRepository.RoutingDataChangedListener> manager = new ListenerManager<>();

  @Test
  public void testSubscribeAndUnsubscribe()
      throws InterruptedException {
    String key = "test";
    TestListener listener1 = new TestListener();
    manager.subscribe(key, listener1);
    manager.trigger(key, new Function<RoutingDataRepository.RoutingDataChangedListener, Void>() {
      @Override
      public Void apply(RoutingDataRepository.RoutingDataChangedListener listener) {
        listener.onRoutingDataChanged(new PartitionAssignment(key, 1));
        return null;
      }
    });
    // Wait thread pool executor to execute.
    Thread.sleep(500);
    Assert.assertTrue(listener1.isExecuted, "The listener should be added and triggered before");
    TestListener listener2 = new TestListener();
    manager.subscribe(key, listener2);
    manager.unsubscribe(key, listener2);
    manager.trigger(key, new Function<RoutingDataRepository.RoutingDataChangedListener, Void>() {
      @Override
      public Void apply(RoutingDataRepository.RoutingDataChangedListener listener) {
        listener.onRoutingDataDeleted(key);
        return  null;
      }
    });
    Thread.sleep(500);
    Assert.assertFalse(listener2.isExecuted, "The listener is unsubscribed before being triggered,");
  }

  private class TestListener implements RoutingDataRepository.RoutingDataChangedListener {
    private boolean isExecuted = false;

    @Override
    public void onRoutingDataChanged(PartitionAssignment partitionAssignment) {
      isExecuted = true;
    }

    @Override
    public void onRoutingDataDeleted(String kafkaTopic) {
     isExecuted = true;
    }
  }
}
