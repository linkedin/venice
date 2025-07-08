package com.linkedin.venice.writer;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.DataProviderUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ChainedCallbackTest {
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSetDependentFuture(boolean hasException) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    CompletableFutureCallback callback = new CompletableFutureCallback(future);

    CompletableFuture<Void> dependentFuture1 = new CompletableFuture<>();
    CompletableFuture<Void> dependentFuture2 = new CompletableFuture<>();
    CompletableFutureCallback callback1 = new CompletableFutureCallback(dependentFuture1);
    CompletableFutureCallback callback2 = new CompletableFutureCallback(dependentFuture2);
    List<PubSubProducerCallback> list = new ArrayList<>();
    list.add(callback1);
    list.add(callback2);
    ChainedPubSubCallback chainedPubSubCallback = new ChainedPubSubCallback(callback, list);
    callback.setCallback(mock(PubSubProducerCallback.class));
    callback1.setCallback(mock(PubSubProducerCallback.class));
    callback2.setCallback(mock(PubSubProducerCallback.class));
    if (hasException) {
      chainedPubSubCallback.onCompletion(null, new VeniceException("Test"));
      Assert.assertTrue(future.isCompletedExceptionally());
      Assert.assertTrue(dependentFuture1.isCompletedExceptionally());
      Assert.assertTrue(dependentFuture2.isCompletedExceptionally());
    } else {
      chainedPubSubCallback.onCompletion(null, null);
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(dependentFuture1.isDone());
      Assert.assertTrue(dependentFuture2.isDone());
    }
  }
}
