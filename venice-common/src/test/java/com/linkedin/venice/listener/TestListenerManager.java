package com.linkedin.venice.listener;

import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestListenerManager {
  private ListenerManager<RoutingDataRepository.RoutingDataChangedListener> manager = new ListenerManager<>();
  private final int TEST_TIME_OUT = 500;

  @Test
  public void testSubscribeAndUnsubscribe()
      throws InterruptedException {
    String key = "testSubscribeAndUnsubscribe";
    TestListener listener1 = new TestListener();
    manager.subscribe(key, listener1);
    manager.trigger(key, new Function<RoutingDataRepository.RoutingDataChangedListener, Void>() {
      @Override
      public Void apply(RoutingDataRepository.RoutingDataChangedListener listener) {
        listener.onRoutingDataChanged(new PartitionAssignment(key, 1));
        return null;
      }
    });
    TestUtils.waitForNonDeterministicCompletion(TEST_TIME_OUT, TimeUnit.MILLISECONDS, () -> listener1.isExecuted);

    TestListener listener2 = new TestListener();
    manager.subscribe(key, listener2);
    manager.unsubscribe(key, listener2);
    Assert.assertEquals(manager.getListenerMap().get(key).size(), 1, "The listener2 is unsubscribed.");
    Assert.assertEquals(manager.getListenerMap().get(key).iterator().next(), listener1,
        "The listener2 is unsubscribed.");
  }

  @Test
  public void testSubscribeAndUnsubscribeWithWildChar()
      throws InterruptedException {
    String key = "testSubscribeAndUnsubscribeWithWildChar";
    TestListener listener1 = new TestListener();
    TestListener listener2 = new TestListener();
    manager.subscribe(Utils.WILDCARD_MATCH_ANY, listener1);
    manager.subscribe(key, listener2);
    manager.trigger(key, new Function<RoutingDataRepository.RoutingDataChangedListener, Void>() {
      @Override
      public Void apply(RoutingDataRepository.RoutingDataChangedListener listener) {
        listener.onRoutingDataChanged(new PartitionAssignment(key, 1));
        return null;
      }
    });
    // Both listeners should be triggered.
    TestUtils.waitForNonDeterministicCompletion(TEST_TIME_OUT, TimeUnit.MILLISECONDS, () -> listener1.isExecuted);
    TestUtils.waitForNonDeterministicCompletion(TEST_TIME_OUT, TimeUnit.MILLISECONDS, () -> listener2.isExecuted);
    listener1.isExecuted = false;
    listener2.isExecuted = false;
    manager.unsubscribe(Utils.WILDCARD_MATCH_ANY, listener1);

    manager.trigger(key, new Function<RoutingDataRepository.RoutingDataChangedListener, Void>() {
      @Override
      public Void apply(RoutingDataRepository.RoutingDataChangedListener listener) {
        listener.onRoutingDataDeleted(key);
        return null;
      }
    });
    // Listener registered with key should be triggered, but the one registered with wild char would not because it already unsubscribed.
    TestUtils.waitForNonDeterministicCompletion(TEST_TIME_OUT, TimeUnit.MILLISECONDS, () -> listener2.isExecuted);
    TestUtils.waitForNonDeterministicCompletion(TEST_TIME_OUT, TimeUnit.MILLISECONDS, () -> !listener1.isExecuted);
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
