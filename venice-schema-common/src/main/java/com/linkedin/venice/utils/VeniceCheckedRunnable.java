package com.linkedin.venice.utils;

/**
 * A Runnable that throws checked exceptions.
 */
@FunctionalInterface
public interface VeniceCheckedRunnable {
  void run() throws Throwable;
}
