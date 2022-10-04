package com.linkedin.alpini.base.pool;

import com.linkedin.alpini.base.concurrency.ExecutorService;
import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.Shutdownable;
import com.linkedin.alpini.base.registry.ShutdownableResource;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@Test(groups = "unit")
public class TestAsyncPool {
  private ExecutorService _executor;

  @BeforeClass(groups = "unit")
  public void beforeClass() {
    _executor = Executors.newSingleThreadExecutor();
  }

  class Element {

  }

  interface LifeCycle extends AsyncPool.LifeCycle<Element> {
  }

  public void testBasicPool() throws InterruptedException {

    ResourceRegistry registry = new ResourceRegistry();

    LifeCycle lifeCycle = Mockito.mock(LifeCycle.class);

    Mockito.when(lifeCycle.create()).thenAnswer(invoke -> CompletableFuture.supplyAsync(Element::new));
    Mockito.when(lifeCycle.testOnRelease(Mockito.any(Element.class)))
        .thenAnswer(invoke -> CompletableFuture.supplyAsync(() -> true));
    Mockito.when(lifeCycle.testAfterIdle(Mockito.any(Element.class)))
        .thenAnswer(invoke -> CompletableFuture.supplyAsync(() -> true));
    Mockito.when(lifeCycle.destroy(Mockito.any(Element.class)))
        .thenAnswer(invoke -> CompletableFuture.supplyAsync(() -> null));

    AsyncPool<Element> pool = AsyncPool.create(lifeCycle, _executor, 1, 10, 10, 100, 30000, TimeUnit.MILLISECONDS);

    registry.register((ShutdownableResource) pool);

    Mockito.verifyNoMoreInteractions(lifeCycle);

    pool.start();
    Thread.sleep(100);

    registry.shutdown();
    registry.waitForShutdown();

    Mockito.verify(lifeCycle, Mockito.times(10)).create();
    Mockito.verify(lifeCycle, Mockito.times(10)).destroy(Mockito.any(Element.class));
    Mockito.verify(lifeCycle).shutdown();

    Mockito.verifyNoMoreInteractions(lifeCycle);
  }

  public void testDefaultShutdown() throws Exception {
    abstract class AbstractPool implements AsyncPool<Element>, Shutdownable {
    }

    AsyncPool<Element> pool =
        Mockito.mock(AbstractPool.class, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));

    CompletableFuture<Void> shutdownFuture = pool.shutdownPool();

    shutdownFuture.get(1, TimeUnit.SECONDS);

    ((Shutdownable) Mockito.verify(pool)).shutdown();
    ((Shutdownable) Mockito.verify(pool)).waitForShutdown();

  }

  public void testNoOpShutdown() throws Exception {
    abstract class AbstractPool implements AsyncPool<Element> {
    }

    AsyncPool<Element> pool =
        Mockito.mock(AbstractPool.class, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));

    CompletableFuture<Void> shutdownFuture = pool.shutdownPool();

    shutdownFuture.get(0, TimeUnit.SECONDS);

  }
}
