package com.linkedin.venice.utils;

import java.util.concurrent.ThreadLocalRandom;


public class RandomGenUtils {
  /**
   * Generate an array of random bytes
   *
   * @param length length of the byte array to be generated
   * @return
   */
  public static byte[] getRandomBytes(int length) {
    byte[] bytes = new byte[length];
    ThreadLocalRandom.current().nextBytes(bytes);
    return bytes;
  }

  /**
   * @return a random float
   */
  public static float getRandomFloat() {
    return ThreadLocalRandom.current().nextFloat();
  }

  /**
   * Return a random integer between 0 and limit (exclusive)
   *
   * @param limit The upper bound for the range of numbers
   * @return
   */
  public static int getRandomIntWithin(int limit) {
    return ThreadLocalRandom.current().nextInt(limit);
  }

  /**
   *
   * @param min Minimum Value
   * @param max Maximum value. max must be greater than min.
   * @return Integer between min and max, inclusive.
   */
  public static int getRandomIntInRange(int min, int max) {
    return min + ThreadLocalRandom.current().nextInt((max - min) + 1);
  }
}
