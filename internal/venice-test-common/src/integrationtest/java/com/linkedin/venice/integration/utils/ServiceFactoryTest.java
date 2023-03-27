package com.linkedin.venice.integration.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;


/**
 * TODO: Delete once experiments are done
 */
public class ServiceFactoryTest {
  private static final Logger LOGGER = LogManager.getLogger(ServiceFactoryTest.class);

  public void sayHello() {
    System.out.println("###Hello, folks!");
  }

  @Test
  public void simpleLoadingTest() {
    ServiceFactory.testRuntimeFactoryLoading();
  }

  public static void main(String[] args) throws InterruptedException {
  }
}
