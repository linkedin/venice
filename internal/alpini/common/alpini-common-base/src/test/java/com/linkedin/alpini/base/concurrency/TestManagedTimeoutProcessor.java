package com.linkedin.alpini.base.concurrency;

import com.linkedin.alpini.base.registry.ResourceRegistry;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestManagedTimeoutProcessor {
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
