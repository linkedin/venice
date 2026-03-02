package com.linkedin.venice.testng;

import com.linkedin.venice.utils.TestUtils;
import java.io.File;
import java.io.FileWriter;
import java.util.Collection;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestNGMethod;


public class VeniceSuiteListener implements ISuiteListener {
  private static final String TIMEOUT_PROPERTY = "venice.test.class.timeout.ms";
  private volatile Thread watchdogThread;

  @Override
  public void onStart(ISuite suite) {
    System.out.println("Start suite " + suite.getName());
    TestUtils.preventSystemExit();
    Configurator.setAllLevels("com.linkedin.d2", Level.OFF);
    startWatchdog(getTestClassName(suite));
  }

  @Override
  public void onFinish(ISuite suite) {
    stopWatchdog();
    System.out.println("Finish suite " + suite.getName());
    TestUtils.restoreSystemExit();
  }

  private void startWatchdog(String suiteName) {
    long timeoutMs = Long.parseLong(System.getProperty(TIMEOUT_PROPERTY, "0"));
    if (timeoutMs <= 0) {
      return;
    }
    long startTime = System.currentTimeMillis();
    Thread thread = new Thread(() -> {
      try {
        Thread.sleep(timeoutMs);
      } catch (InterruptedException e) {
        // Test finished before timeout — normal exit
        return;
      }
      long elapsed = System.currentTimeMillis() - startTime;
      String msg = String.format(
          "TEST CLASS TIMEOUT: %s exceeded the %d second limit (ran for %d seconds).%n"
              + "Split this test class into smaller classes or optimize the slow test methods.%n"
              + "Set -D%s=0 to disable this check locally.",
          suiteName,
          timeoutMs / 1000,
          elapsed / 1000,
          TIMEOUT_PROPERTY);
      System.err.println(msg);
      System.err.flush();
      writeTimeoutMarker(suiteName, timeoutMs / 1000, elapsed / 1000);
      // Remove SecurityManager before halt — preventSystemExit() installs one that blocks non-zero exits
      TestUtils.restoreSystemExit();
      Runtime.getRuntime().halt(1);
    }, "venice-test-watchdog-" + suiteName);
    thread.setDaemon(true);
    thread.start();
    watchdogThread = thread;
  }

  private void stopWatchdog() {
    Thread thread = watchdogThread;
    if (thread != null) {
      thread.interrupt();
      watchdogThread = null;
    }
  }

  /**
   * Extract the test class name from the suite. TestNG names the suite "Gradle suite" by default,
   * but the actual class name is available from the test methods.
   */
  private static String getTestClassName(ISuite suite) {
    Collection<ITestNGMethod> methods = suite.getAllMethods();
    if (methods != null && !methods.isEmpty()) {
      return methods.iterator().next().getTestClass().getName();
    }
    return suite.getName();
  }

  private static void writeTimeoutMarker(String suiteName, long limitSeconds, long elapsedSeconds) {
    try {
      File markerDir = new File("build/test-watchdog-timeouts");
      markerDir.mkdirs();
      File marker = new File(markerDir, suiteName + ".timeout");
      try (FileWriter fw = new FileWriter(marker)) {
        fw.write(suiteName + "|" + elapsedSeconds + "|" + limitSeconds + "\n");
      }
    } catch (Exception e) {
      // Best effort — don't let marker writing prevent the halt
    }
  }
}
