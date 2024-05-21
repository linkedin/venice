package com.linkedin.venice.testng;

import org.testng.ITestResult;
import org.testng.annotations.Test;


/**
 * This test class reproduces an issue of interrupt contamination across tests. This issue is fixed by a listener added
 * to all unit and integration tests which clears the interrupt state:
 *
 * {@link com.linkedin.venice.testng.VeniceTestListener#onTestStart(ITestResult)}
 *
 * N.B.: The naming of the tests is important, as they get executed in lexicographical order.
 */
public class TestInterruptContaminationClearing {
  @Test
  void test1() {
    Thread.currentThread().interrupt();
  }

  @Test
  void test2() throws InterruptedException {
    Thread.sleep(1000);
  }
}
