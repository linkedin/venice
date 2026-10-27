package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.registry.ResourceRegistry;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestManagedTimeoutProcessor {
  @DataProvider
  public Object[][] eventStores() {
    return new Object[][] { { true }, { false } };
  }

  @Test(groups = "unit", dataProvider = "eventStores")
  public void testSchedulingAndCancellationAfterShutdown(boolean useTreeMap) throws Exception {
    TimeoutProcessor processor = TimeoutProcessor.createManaged(useTreeMap, 1);
    try {
      TimeoutProcessor.TimeoutFuture pending = processor.schedule(() -> {}, 1, TimeUnit.DAYS);
      processor.shutdownNow();
      processor.awaitTermination(10, TimeUnit.SECONDS);

      // A later deadline used to bypass the executor and silently enter the stopped timer.
      Assert.assertThrows(RejectedExecutionException.class, () -> processor.schedule(() -> {}, 2, TimeUnit.DAYS));
      Assert.assertThrows(RejectedExecutionException.class, () -> processor.schedule(() -> {}, 0, TimeUnit.SECONDS));
      Assert.assertThrows(RejectedExecutionException.class, () -> processor.execute(() -> {}));
      Assert.assertTrue(pending.cancel());
      Assert.assertTrue(pending.isDone());
      Assert.assertFalse(pending.cancel());
    } finally {
      processor.shutdownNow();
    }
  }

  @Test(groups = "unit", dataProvider = "eventStores", timeOut = 30_000)
  public void testSchedulingRacesWithShutdown(boolean useTreeMap) throws Exception {
    TimeoutProcessor processor = TimeoutProcessor.createManaged(useTreeMap, 1);
    java.util.concurrent.ExecutorService producers = java.util.concurrent.Executors.newFixedThreadPool(4);
    CountDownLatch started = new CountDownLatch(4);
    CountDownLatch release = new CountDownLatch(1);
    ConcurrentLinkedQueue<TimeoutProcessor.TimeoutFuture> accepted = new ConcurrentLinkedQueue<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try {
      for (int i = 0; i < 4; i++) {
        producers.execute(() -> {
          try {
            accepted.add(processor.schedule(() -> {}, 1, TimeUnit.DAYS));
            started.countDown();
            release.await();
            for (int n = 0; n < 1000; n++) {
              accepted.add(processor.schedule(() -> {}, 2, TimeUnit.DAYS));
            }
          } catch (RejectedExecutionException expected) {
            // Shutdown won the race.
          } catch (Throwable t) {
            failure.compareAndSet(null, t);
          }
        });
      }
      Assert.assertTrue(started.await(10, TimeUnit.SECONDS));
      release.countDown();
      processor.shutdownNow();
      producers.shutdown();
      Assert.assertTrue(producers.awaitTermination(10, TimeUnit.SECONDS));
      Assert.assertNull(failure.get());
      Assert.assertThrows(RejectedExecutionException.class, () -> processor.schedule(() -> {}, 2, TimeUnit.DAYS));
      for (TimeoutProcessor.TimeoutFuture future: accepted) {
        Assert.assertTrue(future.cancel());
        Assert.assertTrue(future.isDone());
      }
    } finally {
      release.countDown();
      producers.shutdownNow();
      processor.shutdownNow();
      processor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test(groups = "unit")
  public void testOnlyManagedProcessorSurvivesGlobalShutdown() throws Exception {
    Set<String> classpath = new LinkedHashSet<>();
    for (ClassLoader loader = getClass().getClassLoader(); loader != null; loader = loader.getParent()) {
      if (loader instanceof URLClassLoader) {
        for (URL url: ((URLClassLoader) loader).getURLs()) {
          classpath.add(new File(url.toURI()).getPath());
        }
      }
    }
    classpath.add(System.getProperty("java.class.path"));
    Process process = new ProcessBuilder(
        new File(System.getProperty("java.home"), "bin/java").getPath(),
        "-cp",
        String.join(File.pathSeparator, classpath),
        getClass().getName()).inheritIO().start();
    try {
      Assert.assertTrue(process.waitFor(45, TimeUnit.SECONDS), "Isolated shutdown regression timed out");
      Assert.assertEquals(process.exitValue(), 0, "Isolated shutdown regression failed");
    } finally {
      process.destroyForcibly();
    }
  }

  /** Child JVM: globalShutdown() is process wide, so it cannot run inside the shared test JVM. */
  public static void main(String[] args) throws Exception {
    TimeoutProcessor managed = TimeoutProcessor.createManaged(true, 1);
    TimeoutProcessor registered = new TimeoutProcessor(null, true, 1);
    try {
      ResourceRegistry.globalShutdown();
      registered.awaitTermination(10, TimeUnit.SECONDS);
      Assert.assertThrows(RejectedExecutionException.class, () -> registered.execute(() -> {}));

      CompletableFuture<Void> afterGlobalShutdown = new CompletableFuture<>();
      managed.schedule(() -> afterGlobalShutdown.complete(null), 0, TimeUnit.MILLISECONDS);
      afterGlobalShutdown.get(10, TimeUnit.SECONDS);
      managed.shutdownNow();
      managed.awaitTermination(10, TimeUnit.SECONDS);
      Assert.assertThrows(RejectedExecutionException.class, () -> managed.execute(() -> {}));
    } finally {
      managed.shutdownNow();
      registered.shutdownNow();
    }
  }
}
