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


public class CompletableFutureCallbackTest {
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testSetDependentFuture(boolean hasException) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    CompletableFutureCallback callback = new CompletableFutureCallback(future);

    CompletableFuture<Void> dependentFuture1 = new CompletableFuture<>();
    CompletableFuture<Void> dependentFuture2 = new CompletableFuture<>();
    List<CompletableFuture<Void>> list = new ArrayList<>();
    list.add(dependentFuture1);
    list.add(dependentFuture2);
    callback.setDependentFutureList(list);

    Assert.assertEquals(callback.getDependentFutureList().size(), 2);
    callback.setCallback(mock(PubSubProducerCallback.class));
    if (hasException) {
      callback.onCompletion(null, new VeniceException("Test"));
      Assert.assertTrue(future.isCompletedExceptionally());
      Assert.assertTrue(dependentFuture1.isCompletedExceptionally());
      Assert.assertTrue(dependentFuture2.isCompletedExceptionally());
    } else {
      callback.onCompletion(null, null);
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(dependentFuture1.isDone());
      Assert.assertTrue(dependentFuture2.isDone());
    }
  }
}
