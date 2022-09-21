package com.linkedin.venice.utils.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class VeniceReentrantReadWriteLockTest {
  private static final Logger LOGGER = LogManager.getLogger(VeniceReentrantReadWriteLock.class);

  @Test
  public void testVeniceLockLogging() {
    try {
      VeniceReentrantReadWriteLock veniceLock = new VeniceReentrantReadWriteLock();
      LOGGER.info("Initial state: {}", veniceLock.toString());
      veniceLock.readLock().lock();
      LOGGER.info("After read lock: {}", veniceLock.toString());
      veniceLock.readLock().unlock();
      LOGGER.info("After read unlock: {}", veniceLock.toString());
      veniceLock.writeLock().lock();
      LOGGER.info("After write lock: {}", veniceLock.toString());
      veniceLock.writeLock().unlock();
      Thread.currentThread().setName("testing thread name");
      LOGGER.info("After write unlock: {}", veniceLock.toString());
      veniceLock.writeLock().lock();
      veniceLock.readLock().lock();
      veniceLock.readLock().lock();
      LOGGER.info("After write lock followed by two read locks: {}", veniceLock.toString());
    } catch (Exception e) {
      Assert.fail("VeniceReentrantReadWriteLock fails!");
    }
  }
}
